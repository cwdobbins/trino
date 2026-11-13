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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.VariantPath;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.DoubleType;
import io.trino.spi.variant.Variant;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.VariantJsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.prunedSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.rowGroupCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeGroupFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.parquet.reader.TestPrunedVariantRead.SCHEMA;
import static io.trino.parquet.reader.TestPrunedVariantRead.SHARED_ROWS;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

final class TestVariantReadBatch
{
    private static final VariantPath STATUS = VariantPath.keys("status");
    private static final VariantPath ITEM = VariantPath.of(VariantPath.Step.key("items"), VariantPath.Step.element(), VariantPath.Step.key("sku"));
    private static final VariantPath ROOT = VariantPath.of();

    @TempDir
    Path directory;

    @Test
    void testLazyChunksRetainLevelsAndReleaseMemory()
            throws IOException
    {
        PrimitiveField first = leaf("first", 7);
        PrimitiveField second = leaf("second", 7); // IDs from separate schema views may collide.
        AtomicInteger reads = new AtomicInteger();
        var builder = BIGINT.createBlockBuilder(null, 2);
        BIGINT.writeLong(builder, 11);
        BIGINT.writeLong(builder, 12);
        ColumnChunk chunk = new ColumnChunk(builder.build(), new int[] {1, 1}, new int[] {0, 0});
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try {
            var batch = new VariantReadBatch(
                    List.of(first, second),
                    _ -> {
                        reads.incrementAndGet();
                        return chunk;
                    },
                    memory.newLocalMemoryContext("test"));
            long emptyBytes = memory.getBytes();
            assertThat(emptyBytes).isPositive();
            assertThat(reads).hasValue(0);
            assertThat(batch.read(first)).isSameAs(chunk);
            assertThat(batch.read(leaf("first", 99))).isSameAs(chunk);
            assertThat(batch.read(second)).isSameAs(chunk);
            assertThat(reads).hasValue(2);
            assertThat(memory.getBytes()).isGreaterThan(emptyBytes + chunk.getBlock().getRetainedSizeInBytes());
            assertThat(batch.read(first).getDefinitionLevels()).isSameAs(chunk.getDefinitionLevels());
            assertThat(batch.read(first).getRepetitionLevels()).isSameAs(chunk.getRepetitionLevels());
            batch.reset();
            assertThat(batch.decodedLeaves()).isZero();
            assertThat(memory.getBytes()).isEqualTo(emptyBytes);
            batch.read(first);
            assertThat(reads).hasValue(3);
            batch.close();
            assertThat(memory.getBytes()).isZero();
            assertThatThrownBy(() -> batch.read(first)).isInstanceOf(IllegalStateException.class);
        }
        finally {
            memory.close();
        }
    }

    @Test
    void testPhysicalIdentityIsIndependentOfRequestedSchemaIds()
    {
        PrimitiveField first = leaf("first", 0);
        PrimitiveField alias = leaf("first", 9);
        PrimitiveField distinct = leaf("second", 0);
        assertThat(ParquetReader.getPrimitiveFields(List.of(first, alias, distinct))).containsExactly(first, distinct);
        PrimitiveField incompatible = new PrimitiveField(DoubleType.DOUBLE, false, first.getDescriptor(), 1);
        assertThatThrownBy(() -> ParquetReader.getPrimitiveFields(List.of(first, incompatible)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Incompatible views");
    }

    @Test
    void testIndependentPrunedAndWholeViewsShareLeavesAcrossSelectionsAndRowGroups()
            throws IOException
    {
        List<org.apache.parquet.variant.Variant> rows = new ArrayList<>();
        for (int i = 0; i < 1200; i++) {
            rows.add(i % 19 == 0 ? null : VariantJsonParser.parseJson(SHARED_ROWS.get(i % SHARED_ROWS.size())));
        }
        File file = writeVariantFile(directory, "shared.parquet", SCHEMA, rows, 1024, 512);
        assertThat(rowGroupCount(file)).isGreaterThan(1);
        List<Variant> expected = readFile(file);
        List<MessageType> schemas = List.of(prunedSchema(SCHEMA, List.of(STATUS)), prunedSchema(SCHEMA, List.of(ITEM)), SCHEMA);
        List<Field> views = schemas.stream().map(schema -> field(SCHEMA, schema, "v")).toList();
        assertThat(ParquetReader.getPrimitiveFields(views)).hasSize(SCHEMA.getColumns().size());
        for (boolean selective : List.of(false, true)) {
            for (List<Integer> order : List.of(List.of(0, 1, 2), List.of(2, 1, 0))) {
                AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
                try {
                    try (ParquetReader reader = open(file, views, selective, memory)) {
                        int start = 0;
                        int batch = 0;
                        SourcePage previous = null;
                        SourcePage page;
                        while ((page = reader.nextPage()) != null) {
                            assertThat(reader.variantLeavesDecoded()).isZero();
                            if (previous != null) {
                                SourcePage stale = previous;
                                assertThatThrownBy(() -> stale.getBlock(0)).isInstanceOf(IllegalStateException.class);
                            }
                            previous = page;
                            int count = page.getPositionCount();
                            if (batch++ % 5 == 0) {
                                // Leave all streams unread through several batches/row-group transitions.
                                start += count;
                                continue;
                            }
                            int first = order.getFirst();
                            page.getBlock(first);
                            assertThat(reader.variantLeavesDecoded()).isEqualTo(schemas.get(first).getColumns().size());
                            // Select in original-row coordinates after one view has loaded; include duplicates/reversal and a nonzero offset.
                            int[] positions = {-1, count - 1, 0, count - 1, -1};
                            page.selectPositions(positions, 1, 3);
                            for (int channel : order) {
                                Block block = page.getBlock(channel);
                                VariantPath path = List.of(STATUS, ITEM, ROOT).get(channel);
                                for (int row = 0; row < 3; row++) {
                                    Variant actual = block.isNull(row) ? null : VARIANT.getObject(block, row);
                                    assertThat(same(extract(actual, path), extract(expected.get(start + positions[row + 1]), path)))
                                            .as("batch %s, channel %s, selected row %s", batch, channel, row).isTrue();
                                }
                            }
                            assertThat(reader.variantLeavesDecoded()).isEqualTo(SCHEMA.getColumns().size());
                            page.selectPositions(new int[] {-1, 2, 0, -1}, 1, 2);
                            assertThat(page.getBlock(2).getPositionCount()).isEqualTo(2);
                            assertThat(reader.variantLeavesDecoded()).isEqualTo(SCHEMA.getColumns().size());
                            start += count;
                        }
                        assertThat(start).isEqualTo(rows.size());
                    }
                    assertThat(memory.getBytes()).isZero();
                }
                finally {
                    memory.close();
                }
            }
        }
    }

    @Test
    void testBorrowedPageExpiresAtEofAndClose()
            throws IOException
    {
        File file = ShreddedVariantTestUtils.writeFile(directory, "lifetime.parquet", SCHEMA, SHARED_ROWS);
        for (boolean advanceToEof : List.of(false, true)) {
            AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
            try {
                SourcePage last;
                try (ParquetReader reader = open(file, List.of(field(SCHEMA, SCHEMA, "v")), true, memory)) {
                    last = reader.nextPage();
                    assertThat(last).isNotNull();
                    if (advanceToEof) {
                        SourcePage page;
                        while ((page = reader.nextPage()) != null) {
                            last = page;
                        }
                        SourcePage expired = last;
                        assertThatThrownBy(() -> expired.getBlock(0)).isInstanceOf(IllegalStateException.class);
                        assertThat(reader.variantLeavesDecoded()).isZero();
                    }
                    else {
                        last.getBlock(0);
                        assertThat(reader.variantLeavesDecoded()).isPositive();
                    }
                }
                SourcePage expired = last;
                assertThatThrownBy(() -> expired.getBlock(0)).isInstanceOf(IllegalStateException.class);
                assertThat(memory.getBytes()).isZero();
            }
            finally {
                memory.close();
            }
        }
    }

    @Test
    void testEmptySelectionDoesNotDecodeLeaves()
            throws IOException
    {
        File file = ShreddedVariantTestUtils.writeFile(directory, "empty.parquet", SCHEMA, SHARED_ROWS);
        List<Field> views = List.of(field(SCHEMA, prunedSchema(SCHEMA, List.of(STATUS)), "v"), field(SCHEMA, SCHEMA, "v"));
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try (ParquetReader reader = open(file, views, true, memory)) {
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                page.selectPositions(new int[] {-1, -1}, 1, 0);
                assertThat(page.getBlock(0).getPositionCount()).isZero();
                assertThat(page.getBlock(1).getPositionCount()).isZero();
                assertThat(reader.variantLeavesDecoded()).isZero();
            }
        }
        finally {
            memory.close();
        }
    }

    @Test
    void testMultiplePhysicalVariantsHaveSeparateOwners()
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message table {
                  optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional int64 typed_value; }
                  optional group w (VARIANT(1)) { required binary metadata; optional binary value; optional int64 typed_value; }
                }
                """);
        List<Group> rows = new ArrayList<>();
        for (int row = 0; row < 100; row++) {
            Group group = new SimpleGroup(schema);
            group.addGroup("v").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0, 0})).append("typed_value", (long) row);
            group.addGroup("w").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0, 0})).append("typed_value", 100L + row);
            rows.add(group);
        }
        File file = writeGroupFile(directory, "multiple.parquet", schema, rows);
        List<Field> views = List.of(field(schema, schema, "v"), field(schema, schema, "w"), field(schema, schema, "v"));
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try (ParquetReader reader = open(file, views, true, memory)) {
            SourcePage page;
            int start = 0;
            while ((page = reader.nextPage()) != null) {
                Block w = page.getBlock(1);
                assertThat(reader.variantLeavesDecoded()).isEqualTo(3);
                Block v = page.getBlock(2);
                Block alias = page.getBlock(0);
                assertThat(reader.variantLeavesDecoded()).isEqualTo(6);
                for (int row = 0; row < page.getPositionCount(); row++) {
                    assertThat(VARIANT.getObject(w, row).equals(Variant.ofLong(100L + start + row))).isTrue();
                    assertThat(VARIANT.getObject(v, row).equals(Variant.ofLong(start + row))).isTrue();
                    assertThat(VARIANT.getObject(alias, row).equals(VARIANT.getObject(v, row))).isTrue();
                }
                start += page.getPositionCount();
            }
            assertThat(start).isEqualTo(rows.size());
        }
        finally {
            memory.close();
        }
    }

    private static PrimitiveField leaf(String name, int id)
    {
        return new PrimitiveField(BIGINT, false, new ColumnDescriptor(new String[] {"v", name}, Types.optional(INT64).named(name), 0, 1), id);
    }

    static Field field(MessageType file, MessageType requested, String name)
    {
        return constructField(VARIANT, lookupColumnByName(getColumnIO(file, requested), name)).orElseThrow();
    }

    static ParquetReader open(File file, List<Field> fields, boolean selective, AggregatedMemoryContext memory)
            throws IOException
    {
        var options = ParquetReaderOptions.builder().withMaxReadBlockRowCount(37).withShreddedVariantSelectiveRebuild(selective).build();
        var source = new TestingParquetDataSource(Slices.wrappedBuffer(Files.readAllBytes(file.toPath())), options);
        var metadata = MetadataReader.readFooter(source, Optional.empty());
        var schema = metadata.getFileMetaData().getSchema();
        var descriptors = getDescriptors(schema, schema);
        ImmutableList.Builder<RowGroupInfo> groups = ImmutableList.builder();
        long offset = 0;
        for (var group : metadata.getBlocks()) {
            groups.add(new RowGroupInfo(PrunedBlockMetadata.createPrunedColumnsMetadata(group, source.getId(), descriptors), offset, Optional.empty()));
            offset += group.rowCount();
        }
        return new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                fields.stream().map(field -> new Column("view", field)).toList(),
                false,
                groups.build(),
                source,
                UTC,
                memory,
                options,
                exception -> new RuntimeException(exception),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
