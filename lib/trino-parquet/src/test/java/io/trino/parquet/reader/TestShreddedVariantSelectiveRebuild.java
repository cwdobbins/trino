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
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.VariantPath;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.VariantPathExtraction.Extraction;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.VariantJsonParser;
import org.apache.parquet.variant.VariantValueWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertUnsupportedLayout;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.createTempDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.deleteDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.prunedSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.rowGroupCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.toJson;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.typedValueCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeGroupFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeRecords;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.parquet.reader.TestPrunedVariantRead.SCHEMA;
import static io.trino.parquet.reader.TestPrunedVariantRead.SHARED_ROWS;
import static io.trino.parquet.reader.VariantPathExtraction.describe;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Selective rebuild: a top-level shredded variant read under selected positions reconstructs only the selected rows,
 * and gives the block that reading every row and then applying the selection gives. Every comparison runs with the
 * option on and off; off is today's read.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TestShreddedVariantSelectiveRebuild
{
    private static final int ROWS = 30_000;
    // a junk value outside every selection's range: a position read from outside [offset, offset + size) fails the read
    private static final int JUNK = -1;

    // id (int32, the row index), k (int64, minus the row index) and v, the shared shredded variant of the pruning tests
    private static final MessageType ID_K_V_SCHEMA = new MessageType("m", ImmutableList.<org.apache.parquet.schema.Type>builder()
            .add(Types.required(INT32).named("id"))
            .add(Types.required(INT64).named("k"))
            .addAll(SCHEMA.getFields())
            .build());

    private Path directory;
    private List<String> rows;
    private File file;
    // the whole read of v, with no selection: what every selection is compared against
    private List<Variant> whole;

    @BeforeAll
    void writeFile()
    {
        directory = createTempDirectory();
        rows = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            if (i % 7 == 0) {
                // the shapes that break paths, fallbacks into value (status 7, scalars, lists), and a SQL NULL every 91 rows
                rows.add(i % 91 == 0 ? null : SHARED_ROWS.get((i / 7) % SHARED_ROWS.size()));
                continue;
            }
            StringBuilder items = new StringBuilder();
            for (int j = 0; j < i % 4; j++) {
                items.append(j == 0 ? "" : ", ").append("{\"sku\": \"sku-%s-%s\", \"qty\": %s}".formatted(i, j, j));
            }
            rows.add(
                    """
                    {"status": "status-%s", "n": %s, "user": {"tier": "tier-%s", "geo": {"country": "c%s"}}, "items": [%s], "tags": ["t%s"]%s}"""
                            .formatted(i, i % 100, i % 5, i % 13, items, i, i % 3 == 0 ? ", \"zzz\": " + i : ""));
        }
        file = writeVariantFile(directory, "file.parquet", ID_K_V_SCHEMA, parse(rows), 128 * 1024, 4 * 1024);
        assertThat(rowGroupCount(file)).isGreaterThan(1);
        assertThat(typedValueCount(file)).isGreaterThan(100_000);
        whole = readFile(file);
        assertThat(whole).hasSize(ROWS);
        assertThat(whole.stream().filter(variant -> variant == null).count()).isGreaterThan(300);
    }

    @AfterAll
    void deleteFile()
    {
        deleteDirectory(directory);
    }

    @Test
    void testSelectedRowsEqualFilterAfterRebuild()
            throws IOException
    {
        for (Selection selection : Selection.values()) {
            for (int batchSize : new int[] {8192, 997, 61}) {
                for (boolean selective : new boolean[] {true, false}) {
                    String description = "%s, batch size %s, selective %s".formatted(selection, batchSize, selective);
                    List<Integer> selectedRows = new ArrayList<>();
                    List<Variant> actual = new ArrayList<>();
                    try (ParquetReader reader = open(file, UnaryOperator.identity(), selective, batchSize, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
                        int batchStart = 0;
                        SourcePage page;
                        while ((page = reader.nextPage()) != null) {
                            int batchRows = page.getPositionCount();
                            int[] positions = selection.select(batchRows, batchStart);
                            select(page, positions, 3, false);
                            Block block = page.getBlock(0);
                            assertThat(block.getPositionCount()).as(description).isEqualTo(positions.length);
                            for (int i = 0; i < positions.length; i++) {
                                selectedRows.add(batchStart + positions[i]);
                                actual.add(variantAt(block, i));
                            }
                            batchStart += batchRows;
                        }
                        assertThat(batchStart).as(description).isEqualTo(ROWS);
                    }
                    assertLogicallyEqual(actual, selectedRows.stream().map(whole::get).toList(), selectedRows.stream().map(row -> description + ", row " + row + ": " + rows.get(row)).toList());
                }
            }
        }
    }

    @Test
    void testOnlySelectedRowsAreRebuilt()
            throws IOException
    {
        for (Selection selection : Selection.values()) {
            for (boolean selective : new boolean[] {true, false}) {
                try (ParquetReader reader = open(file, UnaryOperator.identity(), selective, 997, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
                    int batchStart = 0;
                    SourcePage page;
                    int batches = 0;
                    while ((page = reader.nextPage()) != null) {
                        int batchRows = page.getPositionCount();
                        assertThat(reader.shreddedVariantRowsReconstructed()).isZero();
                        int[] positions = selection.select(batchRows, batchStart);
                        select(page, positions, 3, false);
                        assertThat(reader.shreddedVariantRowsReconstructed()).isZero();
                        page.getBlock(0);
                        assertThat(reader.shreddedVariantRowsReconstructed())
                                .as("%s, selective %s, batch at %s", selection, selective, batchStart)
                                .isEqualTo(selective ? positions.length : batchRows);
                        // a cached block is not rebuilt
                        page.getBlock(0);
                        assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(selective ? positions.length : batchRows);
                        batchStart += batchRows;
                        batches++;
                    }
                    assertThat(batches).isGreaterThan(1);
                }
            }
        }

        // with no selection, every row is rebuilt whatever the option
        for (boolean selective : new boolean[] {true, false}) {
            try (ParquetReader reader = open(file, UnaryOperator.identity(), selective, 997, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
                SourcePage page = reader.nextPage();
                page.getBlock(0);
                assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(page.getPositionCount());
            }
        }
    }

    @Test
    void testNestedVariantUnaffected()
            throws IOException
    {
        String variant =
                """
                optional group %s (VARIANT(1)) {
                  required binary metadata;
                  optional binary value;
                  optional group typed_value {
                    required group status { optional binary value; optional binary typed_value (STRING); }
                    required group n { optional binary value; optional int32 typed_value (INTEGER(8,true)); }
                  }
                }
                """;
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group r {
                    optional int32 id;
                    %s
                  }
                  optional group a (LIST) {
                    repeated group list {
                      %s
                    }
                  }
                }
                """.formatted(variant.formatted("v"), variant.formatted("element")));
        GroupType variantInRow = schema.getType("r").asGroupType().getType("v").asGroupType();
        GroupType variantInArray = schema.getType("a").asGroupType().getType("list").asGroupType().getType("element").asGroupType();
        List<String> documents = ImmutableList.<String>builder()
                .add("{\"status\": \"ok\", \"n\": 1}", "{\"status\": 7, \"n\": \"x\", \"zzz\": [1]}")
                .addAll(SHARED_ROWS)
                .build();
        List<Consumer<RecordConsumer>> records = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            String document = documents.get(i % documents.size());
            int row = i;
            records.add(consumer -> {
                if (row % 9 != 4) {
                    consumer.startField("r", 0);
                    consumer.startGroup();
                    consumer.startField("id", 0);
                    consumer.addInteger(row);
                    consumer.endField("id", 0);
                    if (row % 10 != 3) {
                        consumer.startField("v", 1);
                        VariantValueWriter.write(consumer, variantInRow, parse(document));
                        consumer.endField("v", 1);
                    }
                    consumer.endGroup();
                    consumer.endField("r", 0);
                }
                if (row % 8 != 0) {
                    consumer.startField("a", 1);
                    consumer.startGroup();
                    if (row % 8 != 1) {
                        consumer.startField("list", 0);
                        for (int element = 0; element < row % 4 + 1; element++) {
                            consumer.startGroup();
                            if (element != 1) {
                                consumer.startField("element", 0);
                                VariantValueWriter.write(consumer, variantInArray, parse(documents.get((row + element) % documents.size())));
                                consumer.endField("element", 0);
                            }
                            consumer.endGroup();
                        }
                        consumer.endField("list", 0);
                    }
                    consumer.endGroup();
                    consumer.endField("a", 1);
                }
            });
        }
        Path nested = Files.createTempDirectory(directory, "nested");
        File nestedFile = writeRecords(nested, "nested.parquet", schema, records);
        assertThat(typedValueCount(nestedFile)).isGreaterThan(1000);

        Type rowType = RowType.rowType(RowType.field("id", INTEGER), RowType.field("v", VARIANT));
        Type arrayType = new ArrayType(VARIANT);
        for (Selection selection : Selection.values()) {
            List<List<Object>> results = new ArrayList<>();
            List<List<Long>> reconstructed = new ArrayList<>();
            for (boolean selective : new boolean[] {true, false}) {
                List<Object> values = new ArrayList<>();
                List<Long> counts = new ArrayList<>();
                try (ParquetReader reader = open(nestedFile, UnaryOperator.identity(), selective, 512, false, ImmutableList.of(rowType, arrayType), ImmutableList.of("r", "a"))) {
                    int batchStart = 0;
                    SourcePage page;
                    while ((page = reader.nextPage()) != null) {
                        int batchRows = page.getPositionCount();
                        int[] positions = selection.select(batchRows, batchStart);
                        select(page, positions, 2, false);
                        Block rowBlock = page.getBlock(0);
                        Block arrayBlock = page.getBlock(1);
                        counts.add(reader.shreddedVariantRowsReconstructed());
                        for (int i = 0; i < positions.length; i++) {
                            values.add(Arrays.asList(batchStart + positions[i], toObject(rowType, rowBlock, i), toObject(arrayType, arrayBlock, i)));
                        }
                        batchStart += batchRows;
                    }
                }
                results.add(values);
                reconstructed.add(counts);
            }
            assertThat(results.get(0)).as(selection.toString()).isEqualTo(results.get(1));
            // a nested variant is rebuilt for the whole batch, as before
            assertThat(reconstructed.get(0)).as(selection.toString()).isEqualTo(reconstructed.get(1));
            assertThat(reconstructed.get(0)).as(selection.toString()).allMatch(count -> count > 0);
        }
    }

    @Test
    void testErrorOnUnselectedRowIsNotRaised()
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group a {
                        optional binary value;
                        optional int64 typed_value;
                      }
                    }
                  }
                }
                """);
        int corruptRow = 7;
        List<SimpleGroup> corruptRows = new ArrayList<>();
        List<SimpleGroup> validRows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            validRows.add(objectRow(schema, i, false));
            corruptRows.add(objectRow(schema, i, i == corruptRow));
        }
        Path files = Files.createTempDirectory(directory, "corrupt");
        File corrupt = writeGroupFile(files, "corrupt.parquet", schema, corruptRows);
        File valid = writeGroupFile(files, "valid.parquet", schema, validRows);
        // every row's a is in typed_value, so the read goes through the shredded path
        assertThat(typedValueCount(corrupt)).isEqualTo(20);
        assertThat(typedValueCount(valid)).isEqualTo(20);
        List<Variant> expected = readFile(valid);

        int[] withoutCorrupt = {0, 3, 6, 8, 12, 19};
        // a read with no selection, or one that selects the corrupt row, fails as it always has
        assertUnsupportedLayout(() -> readFile(corrupt), "v");
        assertUnsupportedLayout(() -> readSelected(corrupt, true, new int[] {2, corruptRow, 9}), "v");
        assertUnsupportedLayout(() -> readSelected(corrupt, false, new int[] {2, corruptRow, 9}), "v");
        // with the option off, the rejected row is still decoded and still fails
        assertUnsupportedLayout(() -> readSelected(corrupt, false, withoutCorrupt), "v");
        // with it on, the rejected row is never decoded
        List<Variant> selected = readSelected(corrupt, true, withoutCorrupt);
        assertLogicallyEqual(selected, Arrays.stream(withoutCorrupt).mapToObj(expected::get).toList(), Arrays.stream(withoutCorrupt).mapToObj(row -> "row " + row).toList());
        assertThat(readSelected(corrupt, true, new int[0])).isEmpty();
    }

    @Test
    void testPrunedAndSelectedTogether()
            throws IOException
    {
        List<List<VariantPath>> pathSets = ImmutableList.of(
                ImmutableList.of(VariantPath.keys("status"), VariantPath.of(key("items"), element(), key("sku"))),
                ImmutableList.of(VariantPath.keys("zzz"), VariantPath.keys("user", "geo", "country")),
                ImmutableList.of(VariantPath.keys("tags"), VariantPath.of(key("tags"), element()), VariantPath.keys("user", "tier")));
        for (List<VariantPath> paths : pathSets) {
            for (Selection selection : Selection.values()) {
                List<List<Variant>> reads = new ArrayList<>();
                List<Integer> selectedRows = new ArrayList<>();
                for (boolean selective : new boolean[] {true, false}) {
                    selectedRows.clear();
                    List<Variant> read = new ArrayList<>();
                    try (ParquetReader reader = open(file, fileSchema -> prunedSchema(fileSchema, paths), selective, 997, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
                        int batchStart = 0;
                        SourcePage page;
                        while ((page = reader.nextPage()) != null) {
                            int batchRows = page.getPositionCount();
                            int[] positions = selection.select(batchRows, batchStart);
                            select(page, positions, 1, false);
                            Block block = page.getBlock(0);
                            assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(selective ? positions.length : batchRows);
                            for (int i = 0; i < positions.length; i++) {
                                selectedRows.add(batchStart + positions[i]);
                                read.add(variantAt(block, i));
                            }
                            batchStart += batchRows;
                        }
                    }
                    reads.add(read);
                }
                // the same blocks with the option on and off, and the pruned contract for every selected row
                assertLogicallyEqual(reads.get(0), reads.get(1), selectedRows.stream().map(rows::get).map(String::valueOf).toList());
                for (VariantPath path : paths) {
                    for (int i = 0; i < selectedRows.size(); i++) {
                        int row = selectedRows.get(i);
                        Extraction expected = extract(whole.get(row), path);
                        Extraction actual = extract(reads.get(0).get(i), path);
                        if (!same(actual, expected)) {
                            fail("path %s of row %s (%s), reading %s under %s: pruned and selected read gives %s, whole read gives %s".formatted(
                                    path, row, rows.get(row), paths, selection, describe(actual), describe(expected)));
                        }
                    }
                }
            }
        }
    }

    @Test
    void testOptionIsOffByDefault()
    {
        // the library reads as stock Trino unless a connector (Iceberg) turns it on
        assertThat(ParquetReaderOptions.defaultOptions().shreddedVariantSelectiveRebuild()).isFalse();
        assertThat(ParquetReaderOptions.builder().build().shreddedVariantSelectiveRebuild()).isFalse();
        assertThat(ParquetReaderOptions.builder().withShreddedVariantSelectiveRebuild(true).build().shreddedVariantSelectiveRebuild()).isTrue();
    }

    @Test
    void testEmptySelectionReadsNothingAndLaterBatchesStayAligned()
            throws IOException
    {
        List<Variant> expected = new ArrayList<>();
        try (ParquetReader reader = open(file, UnaryOperator.identity(), false, 997, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                Block block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    expected.add(variantAt(block, i));
                }
            }
        }
        for (int batchSize : new int[] {997, 8192}) {
            try (ParquetReader reader = open(file, UnaryOperator.identity(), true, batchSize, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
                int batchStart = 0;
                int batch = 0;
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    int batchRows = page.getPositionCount();
                    // every other batch selects nothing; the others select all rows
                    boolean empty = batch % 2 == 0;
                    long before = reader.shreddedVariantRowsReconstructed();
                    select(page, empty ? new int[0] : IntStream.range(0, batchRows).toArray(), 2, false);
                    Block block = page.getBlock(0);
                    if (empty) {
                        assertThat(block.getPositionCount()).isZero();
                        assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(before);
                    }
                    else {
                        assertThat(block.getPositionCount()).isEqualTo(batchRows);
                        assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(before + batchRows);
                        for (int i = 0; i < batchRows; i++) {
                            assertThat(variantAt(block, i)).isEqualTo(expected.get(batchStart + i));
                        }
                    }
                    batchStart += batchRows;
                    batch++;
                }
                assertThat(batchStart).isEqualTo(expected.size());
                assertThat(batch).isGreaterThan(1);
            }
        }
    }

    @Test
    void testSuccessiveSelectionsWithLazyColumns()
            throws IOException
    {
        long survivingNulls = 0;
        long survivingFallbacks = 0;
        long emptyFinalSelections = 0;
        for (boolean selective : new boolean[] {true, false}) {
            for (boolean tryFirst : new boolean[] {false, true}) {
                // the variant loaded after the second selection, then a third selection; or loaded only after the third
                for (boolean variantAfterThird : new boolean[] {false, true}) {
                    for (int batchSize : new int[] {997, 8192}) {
                        String description = "selective %s, trySelectPositions %s, variant after third %s, batch size %s".formatted(selective, tryFirst, variantAfterThird, batchSize);
                        Random random = new Random(batchSize * 31L + (tryFirst ? 7 : 0) + (variantAfterThird ? 3 : 0));
                        try (ParquetReader reader = open(file, UnaryOperator.identity(), selective, batchSize, tryFirst, ImmutableList.of(INTEGER, BIGINT, VARIANT), ImmutableList.of("id", "k", "v"))) {
                            int batchStart = 0;
                            int batch = 0;
                            SourcePage page;
                            while ((page = reader.nextPage()) != null) {
                                String batchDescription = description + ", batch " + batch;
                                int batchRows = page.getPositionCount();
                                assertThat(page.getChannelCount()).isEqualTo(3);

                                // first selection, in batch positions: noncontiguous, empty in every fifth batch
                                int[] first = batch % 5 == 4 ? new int[0] : sparse(random, batchRows, 0.6);
                                select(page, first, 3, tryFirst);
                                int[] survivors = first.clone();
                                assertThat(reader.shreddedVariantRowsReconstructed()).as(batchDescription).isZero();

                                // load an ordinary column under the first selection
                                assertAligned(page, ImmutableList.of(0), survivors, batchStart, batchDescription + ", after the first selection");

                                // second selection, in positions of the current page: empty in every fourth batch
                                int[] second = batch % 4 == 3 ? new int[0] : sparse(random, survivors.length, 0.5);
                                select(page, second, 5, false);
                                survivors = compose(survivors, second);
                                assertThat(reader.shreddedVariantRowsReconstructed()).as(batchDescription).isZero();

                                int[] third = null;
                                if (variantAfterThird) {
                                    third = sparse(random, survivors.length, 0.7);
                                    select(page, third, 2, false);
                                    survivors = compose(survivors, third);
                                    assertThat(reader.shreddedVariantRowsReconstructed()).as(batchDescription).isZero();
                                }

                                // load the still-lazy variant: only the final survivors are rebuilt with the option on
                                assertAligned(page, ImmutableList.of(0, 2), survivors, batchStart, batchDescription + ", the variant loaded");
                                long rebuilt = selective ? survivors.length : batchRows;
                                assertThat(reader.shreddedVariantRowsReconstructed()).as(batchDescription).isEqualTo(rebuilt);

                                if (!variantAfterThird) {
                                    // a third selection over the cached id and variant, with k still lazy
                                    third = sparse(random, survivors.length, 0.7);
                                    select(page, third, 4, false);
                                    survivors = compose(survivors, third);
                                    assertAligned(page, ImmutableList.of(0, 2), survivors, batchStart, batchDescription + ", after the third selection");
                                    assertThat(reader.shreddedVariantRowsReconstructed()).as(batchDescription + ": a cached block is not rebuilt").isEqualTo(rebuilt);
                                }
                                // k, never loaded until now, describes the same rows as the cached columns
                                assertAligned(page, ImmutableList.of(0, 1, 2), survivors, batchStart, batchDescription + ", every column");
                                assertThat(page.getPositionCount()).isEqualTo(survivors.length);
                                assertThat(page.getPage().getPositionCount()).isEqualTo(survivors.length);

                                if (survivors.length == 0) {
                                    emptyFinalSelections++;
                                }
                                for (int survivor : survivors) {
                                    int row = batchStart + survivor;
                                    if (whole.get(row) == null) {
                                        survivingNulls++;
                                    }
                                    else if (row % 7 == 0) {
                                        survivingFallbacks++;
                                    }
                                }
                                batchStart += batchRows;
                                batch++;
                            }
                            assertThat(batchStart).as(description).isEqualTo(ROWS);
                        }
                    }
                }
            }
        }
        assertThat(survivingNulls).isGreaterThan(20);
        assertThat(survivingFallbacks).isGreaterThan(100);
        assertThat(emptyFinalSelections).isGreaterThan(5);
    }

    /**
     * Every listed channel of the page, in order, holds row {@code batchStart + survivors[i]} at position {@code i}:
     * id the row, k minus the row, v the whole read's variant.
     */
    private void assertAligned(SourcePage page, List<Integer> channels, int[] survivors, int batchStart, String description)
    {
        for (int channel : channels) {
            Block block = page.getBlock(channel);
            assertThat(block.getPositionCount()).as("%s, channel %s", description, channel).isEqualTo(survivors.length);
            for (int i = 0; i < survivors.length; i++) {
                int row = batchStart + survivors[i];
                switch (channel) {
                    case 0 -> assertThat(INTEGER.getInt(block, i)).as("%s, id at %s", description, i).isEqualTo(row);
                    case 1 -> assertThat(BIGINT.getLong(block, i)).as("%s, k at %s", description, i).isEqualTo(-(long) row);
                    default -> {
                        Variant expected = whole.get(row);
                        Variant actual = variantAt(block, i);
                        if (expected == null || actual == null) {
                            assertThat(actual).as("%s, v at %s (row %s)", description, i, row).isEqualTo(expected);
                        }
                        else {
                            assertThat(actual).as("%s, v at %s (row %s)", description, i, row).isEqualTo(expected);
                            assertThat(toJson(actual)).as("%s, v JSON at %s (row %s)", description, i, row).isEqualTo(toJson(expected));
                        }
                    }
                }
            }
        }
    }

    // The selections a batch is filtered with, in its own positions; batchStart makes the random ones differ by batch
    private enum Selection
    {
        NONE(_ -> new int[0]),
        ALL(rows -> range(0, rows)),
        SINGLE(rows -> new int[] {rows / 2}),
        FIRST(_ -> new int[] {0}),
        LAST(rows -> new int[] {rows - 1}),
        RUNS(rows -> filter(rows, position -> position % 200 < 50)),
        EVERY_SEVENTH(rows -> filter(rows, position -> position % 7 == 0)),
        REVERSED_WITH_REPEATS(rows -> {
            int[] every = filter(rows, position -> position % 3 == 0);
            int[] reversed = new int[every.length + 1];
            for (int i = 0; i < every.length; i++) {
                reversed[i] = every[every.length - 1 - i];
            }
            reversed[every.length] = every[every.length - 1];
            return reversed;
        }),
        RANDOM(null);

        private final IntFunction<int[]> select;

        Selection(IntFunction<int[]> select)
        {
            this.select = select;
        }

        int[] select(int rows, int batchStart)
        {
            if (select == null) {
                return sparse(new Random(batchStart), rows, 0.3);
            }
            return select.apply(rows);
        }
    }

    private static int[] range(int from, int to)
    {
        int[] positions = new int[to - from];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = from + i;
        }
        return positions;
    }

    private static int[] filter(int rows, IntPredicate keep)
    {
        return Arrays.stream(range(0, rows)).filter(keep).toArray();
    }

    // Ascending positions below rows, each kept with the given probability; noncontiguous as soon as one is dropped
    private static int[] sparse(Random random, int rows, double density)
    {
        return Arrays.stream(range(0, rows)).filter(_ -> random.nextDouble() < density).toArray();
    }

    // The batch positions that survive a selection made in the current page's positions
    private static int[] compose(int[] survivors, int[] selection)
    {
        int[] composed = new int[selection.length];
        for (int i = 0; i < selection.length; i++) {
            composed[i] = survivors[selection[i]];
        }
        return composed;
    }

    /**
     * Selects {@code positions} through a positions array that holds them at {@code offset}, with junk around them, so a
     * read that confuses the array's indexes with positions fails.
     */
    private static void select(SourcePage page, int[] positions, int offset, boolean trySelect)
    {
        int[] array = new int[offset + positions.length + 2];
        Arrays.fill(array, JUNK);
        System.arraycopy(positions, 0, array, offset, positions.length);
        if (!trySelect || !page.trySelectPositions(array, offset, positions.length)) {
            page.selectPositions(array, offset, positions.length);
        }
        // the array is left as it is: Block.getPositions, which a selection applies to loaded blocks, may keep it
    }

    // Reads v with the rows of the file at the given (ascending) row indexes selected in each batch
    private static List<Variant> readSelected(File file, boolean selective, int[] rows)
            throws IOException
    {
        List<Variant> values = new ArrayList<>();
        try (ParquetReader reader = open(file, UnaryOperator.identity(), selective, 1024, false, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
            int batchStart = 0;
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                int batchRows = page.getPositionCount();
                int start = batchStart;
                int[] positions = Arrays.stream(rows).filter(row -> row >= start && row < start + batchRows).map(row -> row - start).toArray();
                select(page, positions, 2, false);
                Block block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    values.add(variantAt(block, i));
                }
                batchStart += batchRows;
            }
            assertThat(batchStart).isEqualTo(20);
        }
        return values;
    }

    private static ParquetReader open(File file, UnaryOperator<MessageType> requestedSchema, boolean selective, int batchSize, boolean forceSelectedPositionsPushdown, List<Type> types, List<String> columns)
            throws IOException
    {
        ParquetReaderOptions options = ParquetReaderOptions.builder()
                .withMaxReadBlockRowCount(batchSize)
                .withShreddedVariantSelectiveRebuild(selective)
                .build();
        ParquetDataSource dataSource = new TestingParquetDataSource(Slices.wrappedBuffer(Files.readAllBytes(file.toPath())), options);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        MessageType requested = requestedSchema.apply(parquetMetadata.getFileMetaData().getSchema());
        return createParquetReader(dataSource, parquetMetadata, requested, options, newSimpleAggregatedMemoryContext(), types, columns, TupleDomain.all(), forceSelectedPositionsPushdown, UTC);
    }

    private static SimpleGroup objectRow(MessageType schema, long a, boolean corrupt)
    {
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes()));
        if (corrupt) {
            // an object header, truncated: it cannot be merged with the shredded field
            v.append("value", Binary.fromConstantByteArray(new byte[] {0x02, 0x05}));
        }
        v.addGroup("typed_value").addGroup("a").append("typed_value", a);
        return row;
    }

    private static Variant variantAt(Block block, int position)
    {
        return block.isNull(position) ? null : (Variant) VARIANT.getObject(block, position);
    }

    // Variants become their JSON text beside the variant, rows and arrays lists; a null position is null
    private static Object toObject(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type == VARIANT) {
            Variant variant = (Variant) VARIANT.getObject(block, position);
            return ImmutableList.of(variant, toJson(variant));
        }
        if (type instanceof RowType rowType) {
            SqlRow row = rowType.getObject(block, position);
            List<Object> fields = new ArrayList<>();
            for (int i = 0; i < rowType.getFields().size(); i++) {
                fields.add(toObject(rowType.getFields().get(i).getType(), row.getRawFieldBlock(i), row.getRawIndex()));
            }
            return fields;
        }
        if (type instanceof ArrayType arrayType) {
            Block elements = arrayType.getObject(block, position);
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < elements.getPositionCount(); i++) {
                values.add(toObject(arrayType.getElementType(), elements, i));
            }
            return values;
        }
        if (type == INTEGER) {
            return INTEGER.getInt(block, position);
        }
        throw new IllegalArgumentException("unexpected type " + type);
    }

    private static List<org.apache.parquet.variant.Variant> parse(List<String> rows)
    {
        List<org.apache.parquet.variant.Variant> variants = new ArrayList<>(rows.size());
        for (String row : rows) {
            variants.add(row == null ? null : parse(row));
        }
        return variants;
    }

    private static org.apache.parquet.variant.Variant parse(String json)
    {
        try {
            return VariantJsonParser.parseJson(json);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
