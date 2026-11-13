/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet.reader;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.parquet.Field;
import io.trino.parquet.ShreddedVariantField;
import io.trino.parquet.VariantPath;
import io.trino.parquet.VariantPredicateField;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.variant.Variant;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.variant.VariantJsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.prunedSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.rowGroupCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeGroupFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.parquet.reader.TestPrunedVariantRead.SCHEMA;
import static io.trino.parquet.reader.TestPrunedVariantRead.SHARED_ROWS;
import static io.trino.parquet.reader.TestVariantReadBatch.field;
import static io.trino.parquet.reader.TestVariantReadBatch.open;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static io.trino.parquet.reader.VariantPathProbe.State.UNRESOLVED;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestVariantPathProbe
{
    @TempDir
    Path directory;

    @Test
    void testProbeAndSparseReferenceAgainstWholeReads()
            throws IOException
    {
        List<org.apache.parquet.variant.Variant> rows = new ArrayList<>();
        for (int row = 0; row < 1300; row++) {
            rows.add(row % 17 == 0 ? null : VariantJsonParser.parseJson(SHARED_ROWS.get(row % SHARED_ROWS.size())));
        }
        File file = writeVariantFile(directory, "probes.parquet", SCHEMA, rows, 1024, 512);
        assertThat(rowGroupCount(file)).isGreaterThan(1);
        List<Variant> expected = readFile(file);
        List<VariantPath> paths = List.of(
                VariantPath.keys("status"),
                VariantPath.keys("user", "tier"),
                VariantPath.of(VariantPath.Step.key("items"), VariantPath.Step.element(), VariantPath.Step.key("sku")));
        for (boolean wholeFirst : List.of(false, true)) {
            List<Field> fields = List.of(field(SCHEMA, prunedSchema(SCHEMA, paths), "v"), field(SCHEMA, SCHEMA, "v"));
            AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
            try {
                VariantReadView previous = null;
                VariantPathProbe previousProbe = null;
                try (ParquetReader reader = open(file, fields, true, memory)) {
                    int start = 0;
                    SourcePage page;
                    while ((page = reader.nextPage()) != null) {
                        if (previous != null) {
                            VariantReadView expired = previous;
                            VariantPathProbe expiredProbe = previousProbe;
                            assertThatThrownBy(() -> expired.reference(0)).isInstanceOf(IllegalStateException.class);
                            assertThatThrownBy(expiredProbe::state).isInstanceOf(IllegalStateException.class);
                        }
                        assertThat(reader.variantLeavesDecoded()).isZero();
                        if (wholeFirst) {
                            page.getBlock(1);
                        }
                        VariantReadView view = reader.variantView((ShreddedVariantField) fields.getFirst());
                        assertThat(view.referenceRows()).isZero();
                        Set<Integer> fallbackRows = new HashSet<>();
                        int count = page.getPositionCount();
                        for (int row = count - 1; row >= 0; row--) {
                            for (VariantPath path : paths) {
                                List<VariantPathProbe> probes = view.probe(path, row);
                                if (probes.stream().anyMatch(probe -> probe.state() == UNRESOLVED)) {
                                    var reference = view.reference(row);
                                    assertThat(view.reference(row)).isSameAs(reference);
                                    fallbackRows.add(row);
                                    assertThat(same(extract(reference.orElse(null), path), extract(expected.get(start + row), path))).isTrue();
                                }
                                else {
                                    assertProbes(probes, extract(expected.get(start + row), path));
                                }
                            }
                        }
                        assertThat(view.referenceRows()).isEqualTo(fallbackRows.size());
                        int views = view.viewsBuilt();
                        assertThat(views).isLessThan(30);
                        long bytes = memory.getBytes();
                        for (int row = 0; row < count; row++) {
                            for (VariantPath path : paths) {
                                view.probe(path, row);
                            }
                        }
                        assertThat(view.viewsBuilt()).isEqualTo(views);
                        assertThat(memory.getBytes()).isEqualTo(bytes);
                        Block whole = page.getBlock(1);
                        for (int row = 0; row < count; row++) {
                            Variant actual = whole.isNull(row) ? null : VARIANT.getObject(whole, row);
                            assertThat(same(extract(actual, VariantPath.of()), extract(expected.get(start + row), VariantPath.of()))).isTrue();
                        }
                        assertThat(reader.variantLeavesDecoded()).isEqualTo(SCHEMA.getColumns().size());
                        page.selectPositions(new int[] {-1, count - 1, 0, count - 1, -1}, 1, 3);
                        assertThat(page.getBlock(0).getPositionCount()).isEqualTo(3);
                        assertThat(view.referenceRows()).isEqualTo(fallbackRows.size());
                        previous = view;
                        previousProbe = view.probe(paths.getFirst(), 0).getFirst();
                        start += count;
                    }
                    assertThat(start).isEqualTo(rows.size());
                    VariantReadView expired = previous;
                    assertThatThrownBy(() -> expired.reference(0)).isInstanceOf(IllegalStateException.class);
                }
                assertThat(memory.getBytes()).isZero();
            }
            finally {
                memory.close();
            }
        }
    }

    @Test
    void testBorrowedViewsExpireOnCancellationAndMemoryFailureReleasesReservations()
            throws IOException
    {
        File file = ShreddedVariantTestUtils.writeFile(directory, "close.parquet", SCHEMA, List.of("{\"status\":\"open\"}"));
        ShreddedVariantField field = (ShreddedVariantField) field(SCHEMA, SCHEMA, "v");
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        VariantReadView view;
        VariantPathProbe probe;
        try (ParquetReader reader = open(file, List.of(field), true, memory)) {
            assertThat(reader.nextPage()).isNotNull();
            view = reader.variantView(field);
            probe = view.probe(VariantPath.keys("status"), 0).getFirst();
            view.reference(0);
            assertThat(memory.getBytes()).isPositive();
            // Closing before EOF models cancellation while borrowed views still exist.
        }
        assertThat(memory.getBytes()).isZero();
        assertThatThrownBy(probe::value).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> view.reference(0)).isInstanceOf(IllegalStateException.class);
        memory.close();

        AtomicLong reserved = new AtomicLong();
        AtomicBoolean reject = new AtomicBoolean();
        AggregatedMemoryContext limited = AggregatedMemoryContext.newRootAggregatedMemoryContext(new MemoryReservationHandler()
        {
            @Override
            public ListenableFuture<Void> reserveMemory(String tag, long delta)
            {
                if (reject.get() && delta > 0 && tag.equals("VariantReadBatch")) {
                    throw new IllegalStateException("test memory limit");
                }
                reserved.addAndGet(delta);
                return Futures.immediateVoidFuture();
            }

            @Override
            public boolean tryReserveMemory(String tag, long delta)
            {
                reserveMemory(tag, delta);
                return true;
            }
        }, 0);
        try (ParquetReader reader = open(file, List.of(field), true, limited)) {
            assertThat(reader.nextPage()).isNotNull();
            reject.set(true);
            assertThatThrownBy(() -> reader.variantView(field)).isInstanceOf(IllegalStateException.class).hasMessage("test memory limit");
        }
        assertThat(limited.getBytes()).isZero();
        assertThat(reserved).hasValue(0);
        limited.close();
    }

    @Test
    void testNullMissingAndConflictingFallbackLiteralCases()
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message table {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      optional group a { optional binary value; optional int64 typed_value; }
                    }
                  }
                }
                """);
        Variant fallback = Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(99), utf8Slice("b"), Variant.ofLong(9)));
        List<Group> rows = new ArrayList<>();
        rows.add(new SimpleGroup(schema)); // SQL NULL
        for (int row = 1; row < 6; row++) {
            Group record = new SimpleGroup(schema);
            Group root = record.addGroup("v");
            root.append("metadata", Binary.fromConstantByteArray(fallback.metadata().toSlice().getBytes()));
            if (row >= 4) {
                root.append("value", Binary.fromConstantByteArray(fallback.data().getBytes()));
            }
            if (row >= 2) {
                Group child = root.addGroup("typed_value").addGroup("a");
                if (row == 3 || row == 4) {
                    child.append("typed_value", 7L);
                }
            }
            rows.add(record);
        }
        File file = writeGroupFile(directory, "irregular.parquet", schema, rows);
        var states = List.of(
                VariantPathProbe.State.SQL_NULL,
                VariantPathProbe.State.WRONG_SHAPE,
                VariantPathProbe.State.MISSING,
                VariantPathProbe.State.TYPED_VALUE,
                UNRESOLVED,
                UNRESOLVED);
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try (ParquetReader reader = open(file, List.of(field(schema, schema, "v")), true, memory)) {
            int start = 0;
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                VariantReadView view = reader.variantView((ShreddedVariantField) field(schema, schema, "v"));
                for (int row = 0; row < page.getPositionCount(); row++) {
                    VariantPathProbe probe = view.probe(VariantPath.keys("a"), row).getFirst();
                    int index = start + row;
                    assertThat(probe.state()).isEqualTo(states.get(index));
                    if (index == 3) {
                        assertThat(probe.value().equals(Variant.ofLong(7))).isTrue();
                        assertThat(view.referenceRows()).isZero();
                    }
                    if (index >= 4) {
                        Variant actual = view.reference(row).orElseThrow().getObjectField(utf8Slice("a")).orElseThrow();
                        assertThat(actual.equals(Variant.ofLong(index == 4 ? 7 : 99))).isTrue();
                    }
                    if (index == 1) {
                        assertThat(view.reference(row).orElseThrow().equals(Variant.NULL_VALUE)).isTrue();
                    }
                }
                start += page.getPositionCount();
            }
            assertThat(start).isEqualTo(rows.size());
        }
        finally {
            memory.close();
        }
    }

    @Test
    void testSparseFallbackCacheOwnsCompactStorage()
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message table {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      optional group a { optional binary value; optional int64 typed_value; }
                    }
                  }
                }
                """);
        Variant fallback = Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofString("payload".repeat(40))));
        List<Group> rows = new ArrayList<>();
        for (int row = 0; row < 1000; row++) {
            Group record = new SimpleGroup(schema);
            record.addGroup("v")
                    .append("metadata", Binary.fromConstantByteArray(fallback.metadata().toSlice().getBytes()))
                    .append("value", Binary.fromConstantByteArray(fallback.data().getBytes()));
            rows.add(record);
        }
        File file = writeGroupFile(directory, "compact-fallback.parquet", schema, rows);
        ShreddedVariantField field = (ShreddedVariantField) field(schema, schema, "v");
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try (ParquetReader reader = open(file, List.of(field), true, memory)) {
            int count = 0;
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                VariantReadView view = reader.variantView(field);
                long before = memory.getBytes();
                for (int row = 0; row < page.getPositionCount(); row++) {
                    Variant cached = view.reference(row).orElseThrow();
                    assertThat(cached.equals(fallback)).isTrue();
                    assertThat(cached.data().getRetainedSize()).isLessThan(cached.data().length() + 128L);
                    assertThat(cached.metadata().toSlice().getRetainedSize()).isLessThan(cached.metadata().toSlice().length() + 128L);
                }
                assertThat(memory.getBytes() - before).isLessThan(page.getPositionCount() * (fallback.data().length() + fallback.metadata().toSlice().length() + 512L));
                count += page.getPositionCount();
            }
            assertThat(count).isEqualTo(rows.size());
        }
        finally {
            memory.close();
        }
    }

    @Test
    void testSyntheticDecisionsAreLazyAndFollowOriginalPositions()
            throws IOException
    {
        List<String> rows = List.of(
                "{\"status\":\"open\",\"user\":{\"tier\":\"large\"}}",
                "{\"status\":\"closed\"}",
                "{}",
                "{\"status\":null}",
                "17",
                "{\"status\":\"open\"}");
        File file = ShreddedVariantTestUtils.writeFile(directory, "decisions.parquet", SCHEMA, rows);
        List<Variant> expected = readFile(file);
        VariantPath path = VariantPath.keys("status");
        Variant wanted = Variant.ofString(utf8Slice("open"));
        MessageType requested = prunedSchema(SCHEMA, List.of(path));
        VariantPredicateEvaluator evaluator = new VariantPredicateEvaluator()
        {
            @Override
            public Decision evaluate(VariantBatchAccess batch, int position)
            {
                VariantPathProbe probe = batch.probe(path, position).getFirst();
                return switch (probe.state()) {
                    case TYPED_VALUE -> probe.value().equals(wanted) ? Decision.TRUE : Decision.FALSE;
                    case UNRESOLVED -> evaluateReference(batch.reference(position).orElse(null));
                    case SQL_NULL, MISSING, WRONG_SHAPE -> Decision.FALSE;
                };
            }

            @Override
            public Decision evaluateReference(Variant document)
            {
                var value = extract(document, path);
                return value instanceof VariantPathExtraction.Extraction.Value actual && actual.value().equals(wanted) ? Decision.TRUE : Decision.FALSE;
            }
        };
        // Predicate-only scans must discover real source leaves even without any ordinary output field.
        for (boolean withWhole : List.of(false, true)) {
            for (boolean emptySelection : List.of(false, true)) {
                Field predicate = new VariantPredicateField(field(SCHEMA, requested, "v"), evaluator);
                List<Field> fields = withWhole ? List.of(predicate, field(SCHEMA, SCHEMA, "v")) : List.of(predicate);
                AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
                try (ParquetReader reader = open(file, fields, true, memory)) {
                    int start = 0;
                    SourcePage page;
                    while ((page = reader.nextPage()) != null) {
                        int count = page.getPositionCount();
                        assertThat(reader.variantLeavesDecoded()).isZero();
                        if (emptySelection) {
                            page.selectPositions(new int[] {-1, -1}, 1, 0);
                            assertThat(page.getBlock(0).getPositionCount()).isZero();
                            assertThat(reader.variantLeavesDecoded()).isZero();
                        }
                        else {
                            int[] positions = {-1, count - 1, 0, count - 1, -1};
                            page.selectPositions(positions, 1, 3);
                            Block decisions = page.getBlock(0);
                            assertThat(reader.variantLeavesDecoded()).isEqualTo(requested.getColumns().size());
                            assertThat(reader.shreddedVariantRowsReconstructed()).isZero();
                            for (int row = 0; row < 3; row++) {
                                assertThat(decisions.isNull(row)).isFalse();
                                assertThat(TINYINT.getLong(decisions, row)).isEqualTo(evaluator.evaluateReference(expected.get(start + positions[row + 1])).code());
                            }
                            page.selectPositions(new int[] {2, 0}, 0, 2);
                            assertThat(page.getBlock(0).getPositionCount()).isEqualTo(2);
                            if (withWhole) {
                                Block whole = page.getBlock(1);
                                assertThat(whole.getPositionCount()).isEqualTo(2);
                                for (int row = 0; row < 2; row++) {
                                    Variant actual = whole.isNull(row) ? null : VARIANT.getObject(whole, row);
                                    assertThat(same(extract(actual, VariantPath.of()), extract(expected.get(start + count - 1), VariantPath.of()))).isTrue();
                                }
                                assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(2);
                                assertThat(reader.variantLeavesDecoded()).isEqualTo(SCHEMA.getColumns().size());
                            }
                        }
                        start += count;
                    }
                    assertThat(start).isEqualTo(rows.size());
                }
                finally {
                    memory.close();
                }
            }
        }
    }

    private static void assertProbes(List<VariantPathProbe> probes, VariantPathExtraction.Extraction expected)
    {
        List<VariantPathExtraction.Extraction> values = expected instanceof VariantPathExtraction.Extraction.Elements elements
                ? elements.perElement() : List.of(expected);
        assertThat(probes).hasSize(values.size());
        for (int i = 0; i < probes.size(); i++) {
            VariantPathProbe probe = probes.get(i);
            var value = values.get(i);
            switch (probe.state()) {
                case SQL_NULL -> assertThat(value).isInstanceOf(VariantPathExtraction.Extraction.SqlNull.class);
                case MISSING -> assertThat(value).isInstanceOf(VariantPathExtraction.Extraction.Missing.class);
                case WRONG_SHAPE -> assertThat(value).isInstanceOf(VariantPathExtraction.Extraction.WrongShape.class);
                case TYPED_VALUE -> {
                    assertThat(value).isInstanceOf(VariantPathExtraction.Extraction.Value.class);
                    assertThat(VariantPathExtraction.sameValue(probe.value(), ((VariantPathExtraction.Extraction.Value) value).value())).isTrue();
                }
                case UNRESOLVED -> throw new AssertionError("Unexpected unresolved probe");
            }
        }
    }
}
