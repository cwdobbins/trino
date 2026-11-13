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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.airlift.slice.Slice;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ShreddedVariantField;
import io.trino.parquet.ShreddedVariantPruning;
import io.trino.parquet.VariantPath;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.variant.VariantJsonParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static io.trino.parquet.reader.GeoMesaVariantCompatibilityIT.assertPath;
import static io.trino.parquet.reader.GeoMesaVariantCompatibilityIT.assertValue;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Explicit reader gate for registered files retained by the exporter's VariantShreddingExporterIT.
 */
public final class ExporterVariantCompatibilityIT
{
    private static final String FIXTURE_DIRECTORY = "geomesa.variant.exporter-fixture-directory";
    private static final List<String> LAYOUTS = List.of("variant_plain", "variant_sampled");
    private static final List<VariantPath> PATHS = List.of(
            VariantPath.keys("n"),
            VariantPath.keys("price"),
            VariantPath.keys("huge"),
            VariantPath.keys("Mixed.Key"),
            VariantPath.keys("mixed.key"),
            VariantPath.keys("later"),
            VariantPath.keys("absent"),
            VariantPath.of(key("arr"), element()));

    public static void main(String[] args)
            throws IOException
    {
        checkArgument(args.length == 1, "Usage: ExporterVariantCompatibilityIT <fixture-directory>");
        System.setProperty(FIXTURE_DIRECTORY, args[0]);
        var gate = new ExporterVariantCompatibilityIT();
        gate.testWholeValuesAndPhysicalPopulation();
        gate.testPrunedAndSelectedValues();
        System.out.println("Exporter VARIANT compatibility: 2 checks passed (whole/physical, pruned/selected)");
    }

    @Test
    void testWholeValuesAndPhysicalPopulation()
            throws IOException
    {
        Map<Integer, Variant> reference = expectedRows(directory(LAYOUTS.getFirst()));
        for (String layout : LAYOUTS) {
            Path directory = directory(layout);
            Map<Integer, Variant> expected = expectedRows(directory);
            assertThat(expected).isEqualTo(reference);
            long typed = 0;
            long fallback = 0;
            Set<String> types = new HashSet<>();
            Set<Integer> ordinals = new HashSet<>();
            for (Path file : files(directory)) {
                try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file), ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
                    assertThat(reader.getFooter().getFileMetaData().getSchema().getType("payload").getId()).isNotNull();
                    for (var group : reader.getFooter().getBlocks()) {
                        for (var column : group.getColumns()) {
                            long populated = column.getValueCount() - column.getStatistics().getNumNulls();
                            if (column.getPath().toDotString().equals("payload.typed_value.n.typed_value")) {
                                typed += populated;
                                types.add(column.getPrimitiveType().getPrimitiveTypeName().name());
                            }
                            if (column.getPath().toDotString().equals("payload.typed_value.n.value")) {
                                fallback += populated;
                            }
                        }
                    }
                }
                for (int ordinal : read(file, expected, List.of(), false, 7, count -> IntStream.range(0, count).toArray())) {
                    assertThat(ordinals.add(ordinal)).as("duplicate exported ordinal %s", ordinal).isTrue();
                }
            }
            assertThat(ordinals).isEqualTo(expected.keySet());
            if (layout.equals("variant_sampled")) {
                assertThat(typed).isPositive();
                assertThat(fallback).as("late value outside the sampled byte layout").isPositive();
                assertThat(types).contains("INT32", "BINARY");
            }
            else {
                assertThat(typed).isZero();
            }
        }
    }

    @Test
    void testPrunedAndSelectedValues()
            throws IOException
    {
        List<List<VariantPath>> requests = new ArrayList<>();
        requests.add(List.of());
        PATHS.forEach(path -> requests.add(List.of(path)));
        requests.add(PATHS);
        requests.add(List.of(VariantPath.of()));
        List<IntFunction<int[]>> selections = List.of(
                _ -> new int[0],
                count -> IntStream.range(0, count).filter(position -> position % 2 == 0).toArray(),
                count -> new int[] {count - 1, 0, count - 1});
        for (String layout : LAYOUTS) {
            Path directory = directory(layout);
            Map<Integer, Variant> expected = expectedRows(directory);
            for (Path file : files(directory)) {
                for (List<VariantPath> paths : requests) {
                    for (boolean selective : List.of(false, true)) {
                        for (int batchSize : new int[] {3, 17}) {
                            for (IntFunction<int[]> selection : selections) {
                                read(file, expected, paths, selective, batchSize, selection);
                            }
                        }
                    }
                }
            }
        }
    }

    private static Path directory(String layout)
    {
        String directory = System.getProperty(FIXTURE_DIRECTORY);
        checkArgument(directory != null && !directory.isBlank(), "Set -D%s to VariantShreddingExporterIT output", FIXTURE_DIRECTORY);
        Path result = Path.of(directory).resolve(layout);
        checkArgument(Files.isRegularFile(result.resolve("expected.jsonl")), "Missing exporter manifest: %s", result);
        return result;
    }

    private static List<Path> files(Path directory)
            throws IOException
    {
        try (var paths = Files.list(directory)) {
            List<Path> files = paths.filter(path -> path.getFileName().toString().endsWith(".parquet")).sorted().toList();
            assertThat(files).hasSize(3);
            return files;
        }
    }

    private static Map<Integer, Variant> expectedRows(Path directory)
            throws IOException
    {
        Map<Integer, Variant> rows = new HashMap<>();
        for (String line : Files.readAllLines(directory.resolve("expected.jsonl"))) {
            try (JsonParser parser = jsonFactory().createParser(line)) {
                assertThat(parser.nextToken()).isEqualTo(JsonToken.START_OBJECT);
                Integer ordinal = null;
                String id = null;
                String json = null;
                boolean payloadPresent = false;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.currentName();
                    parser.nextToken();
                    switch (field) {
                        case "id" -> id = parser.getText();
                        case "ordinal" -> ordinal = parser.getIntValue();
                        case "payload" -> {
                            payloadPresent = true;
                            assertThat(parser.currentToken()).isIn(JsonToken.VALUE_STRING, JsonToken.VALUE_NULL);
                            json = parser.currentToken() == JsonToken.VALUE_NULL ? null : parser.getText();
                        }
                        default -> throw new IllegalArgumentException("Unknown manifest field: " + field);
                    }
                }
                assertThat(parser.nextToken()).isNull();
                assertThat(ordinal).isNotNull();
                assertThat(id).isEqualTo(ordinal.toString());
                assertThat(payloadPresent).isTrue();
                assertThat(rows).doesNotContainKey(ordinal);
                Variant value = null;
                if (json != null) {
                    var parsed = VariantJsonParser.parseJson(json);
                    value = Variant.from(Metadata.from(copy(parsed.getMetadataBuffer())), copy(parsed.getValueBuffer()));
                }
                rows.put(ordinal, value);
            }
        }
        assertThat(rows).hasSize(81);
        return rows;
    }

    private static Slice copy(ByteBuffer buffer)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return wrappedBuffer(bytes);
    }

    private static List<Integer> read(Path file, Map<Integer, Variant> expected, List<VariantPath> paths, boolean selective, int batchSize, IntFunction<int[]> selection)
            throws IOException
    {
        var options = ParquetReaderOptions.builder().withMaxReadBlockRowCount(batchSize).withShreddedVariantSelectiveRebuild(selective).build();
        var source = new TestingParquetDataSource(wrappedBuffer(Files.readAllBytes(file)), options);
        var metadata = MetadataReader.readFooter(source, Optional.empty());
        MessageType schema = metadata.getFileMetaData().getSchema();
        var payload = schema.getType("payload").asGroupType();
        MessageType requested = new MessageType(schema.getName(), List.of(schema.getType("ordinal"), paths.isEmpty() ? payload : ShreddedVariantPruning.prune("payload", payload, paths)));
        var columns = getColumnIO(schema, requested);
        Field ordinalField = constructField(INTEGER, lookupColumnByName(columns, "ordinal")).orElseThrow();
        Field payloadField = constructField(VARIANT, lookupColumnByName(columns, "payload")).orElseThrow();
        var descriptors = getDescriptors(schema, requested);
        List<RowGroupInfo> groups = new ArrayList<>();
        long offset = 0;
        for (var group : metadata.getBlocks()) {
            groups.add(new RowGroupInfo(PrunedBlockMetadata.createPrunedColumnsMetadata(group, source.getId(), descriptors), offset, Optional.empty()));
            offset += group.rowCount();
        }
        var memory = newSimpleAggregatedMemoryContext();
        List<Integer> ordinals = new ArrayList<>();
        try (ParquetReader reader = new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                List.of(new Column("ordinal", ordinalField), new Column("payload", payloadField)),
                false,
                groups,
                source,
                UTC,
                memory,
                options,
                exception -> new RuntimeException(exception),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())) {
            SourcePage page;
            int visited = 0;
            while ((page = reader.nextPage()) != null) {
                int count = page.getPositionCount();
                int[] selected = selection.apply(count);
                Block beforeSelection = page.getBlock(0);
                int[] expectedOrdinals = IntStream.of(selected).map(position -> toIntExact(INTEGER.getLong(beforeSelection, position))).toArray();
                page.selectPositions(selected, 0, selected.length);
                Block actualOrdinals = page.getBlock(0);
                Block values = page.getBlock(1);
                assertThat(values.getPositionCount()).isEqualTo(selected.length);
                for (int position = 0; position < selected.length; position++) {
                    int ordinal = toIntExact(INTEGER.getLong(actualOrdinals, position));
                    assertThat(ordinal).isEqualTo(expectedOrdinals[position]);
                    assertThat(expected).containsKey(ordinal);
                    Variant actual = values.isNull(position) ? null : VARIANT.getObject(values, position);
                    if (paths.isEmpty()) {
                        assertValue(actual, expected.get(ordinal));
                    }
                    else {
                        for (VariantPath path : paths) {
                            assertPath(actual, expected.get(ordinal), path);
                        }
                    }
                    ordinals.add(ordinal);
                }
                if (payloadField instanceof ShreddedVariantField) {
                    assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(selective ? selected.length : count);
                }
                visited += count;
            }
            assertThat(visited).isEqualTo(toIntExact(offset));
        }
        finally {
            memory.close();
        }
        return ordinals;
    }
}
