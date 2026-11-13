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
import io.trino.memory.context.AggregatedMemoryContext;
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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static io.trino.parquet.reader.VariantPathProbe.State.MISSING;
import static io.trino.parquet.reader.VariantPathProbe.State.SQL_NULL;
import static io.trino.parquet.reader.VariantPathProbe.State.TYPED_VALUE;
import static io.trino.parquet.reader.VariantPathProbe.State.UNRESOLVED;
import static io.trino.parquet.reader.VariantPathProbe.State.WRONG_SHAPE;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.ARRAY;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Explicit cross-repository gate. Generate fixtures with GeoMesaVariantFixtureTest in the companion GeoMesa checkout,
 * then run with -Dtest=GeoMesaVariantCompatibilityIT -Dgeomesa.variant.fixture-directory=/path/to/fixtures.
 * The IT suffix keeps this external-fixture gate out of the ordinary unit suite. An explicit invocation without
 * complete fixtures fails instead of silently skipping. No GeoMesa classes enter the Trino runtime classpath.
 */
public final class GeoMesaVariantCompatibilityIT
{
    private static final String FIXTURE_DIRECTORY = "geomesa.variant.fixture-directory";
    private static final String COLUMN = "props";
    private static final List<String> FILES = List.of("plain.parquet", "object.parquet", "array.parquet");
    private static final List<VariantPath> PATHS = List.of(
            VariantPath.keys("Name"),
            VariantPath.keys("name"),
            VariantPath.keys("n"),
            VariantPath.keys("long"),
            VariantPath.keys("nested", "flag"),
            VariantPath.keys("a.b"),
            VariantPath.keys("quote\"key"),
            VariantPath.keys("amount4"),
            VariantPath.keys("amount8"),
            VariantPath.keys("amount16"),
            VariantPath.keys("unknown", "Name"),
            VariantPath.keys("absent"),
            VariantPath.of(key("items"), element(), key("id")),
            VariantPath.of(element(), key("id")),
            VariantPath.of(element(), key("name")),
            VariantPath.of(element(), key("nested"), key("flag")));

    public static void main(String[] args)
            throws IOException
    {
        checkArgument(args.length == 1, "Usage: GeoMesaVariantCompatibilityIT <fixture-directory>");
        System.setProperty(FIXTURE_DIRECTORY, args[0]);
        var gate = new GeoMesaVariantCompatibilityIT();
        gate.testWholeValuesAndPhysicalPopulation();
        gate.testPrunedPaths();
        gate.testSelectiveReconstruction();
        gate.testTypedProbesAndReferenceFallback();
        System.out.println("GeoMesa VARIANT compatibility: 4 checks passed (whole, pruned, selective, typed/fallback)");
    }

    @Test
    void testWholeValuesAndPhysicalPopulation()
            throws IOException
    {
        List<Variant> expected = expectedRows();
        assertThat(expected).isNotEmpty().containsNull();
        for (String name : FILES) {
            Path file = fixture(name);
            try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file), ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
                var schema = reader.getFooter().getFileMetaData().getSchema();
                assertThat(schema.getType(COLUMN).getId()).as("Iceberg field ID").isNotNull();
                var groups = reader.getFooter().getBlocks();
                assertThat(groups).as("row groups in %s", name).hasSizeGreaterThan(1);
                long typed = 0;
                long fallback = 0;
                long sqlNulls = 0;
                boolean multiplePages = false;
                for (var group : groups) {
                    for (var column : group.getColumns()) {
                        String[] path = column.getPath().toArray();
                        if (!path[0].equals(COLUMN)) {
                            continue;
                        }
                        assertThat(column.getStatistics().isNumNullsSet()).isTrue();
                        long populated = column.getValueCount() - column.getStatistics().getNumNulls();
                        switch (path[path.length - 1]) {
                            case "typed_value" -> typed += populated;
                            case "value" -> fallback += populated;
                            case "metadata" -> {
                                sqlNulls += column.getStatistics().getNumNulls();
                                multiplePages |= reader.readOffsetIndex(column).getPageCount() > 1;
                            }
                        }
                    }
                }
                assertThat(sqlNulls).isEqualTo(expected.stream().filter(value -> value == null).count());
                assertThat(multiplePages).as("multiple pages within a row group in %s", name).isTrue();
                assertThat(fallback).as("populated fallback in %s", name).isPositive();
                if (name.equals("plain.parquet")) {
                    assertThat(typed).isZero();
                }
                else {
                    assertThat(typed).as("populated typed leaves in %s", name).isPositive();
                }
            }
            for (int batchSize : new int[] {3, 17, 37}) {
                assertRows(read(file, List.of(), false, batchSize), expected);
            }
        }
    }

    @Test
    void testPrunedPaths()
            throws IOException
    {
        List<Variant> expected = expectedRows();
        List<List<VariantPath>> requests = new ArrayList<>();
        PATHS.forEach(path -> requests.add(List.of(path)));
        requests.add(PATHS);
        requests.add(List.of(VariantPath.of()));
        for (String name : FILES) {
            for (List<VariantPath> paths : requests) {
                List<Variant> actual = read(fixture(name), paths, true, 7);
                assertThat(actual).hasSameSizeAs(expected);
                for (int row = 0; row < actual.size(); row++) {
                    for (VariantPath path : paths) {
                        assertObservations(observe(actual.get(row), path), observe(expected.get(row), path));
                    }
                }
            }
        }
    }

    @Test
    void testSelectiveReconstruction()
            throws IOException
    {
        List<Variant> expected = expectedRows();
        List<IntFunction<int[]>> selections = List.of(
                _ -> new int[0],
                count -> IntStream.range(0, count).filter(position -> position % 2 == 0).toArray(),
                count -> new int[] {count - 1, 0, count - 1});
        for (String name : FILES) {
            for (boolean selective : List.of(false, true)) {
                for (int batchSize : new int[] {3, 17}) {
                    for (IntFunction<int[]> selection : selections) {
                        for (List<VariantPath> paths : List.of(List.<VariantPath>of(), PATHS)) {
                            AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
                            try (ParquetReader reader = open(fixture(name), paths, selective, batchSize, memory)) {
                                int offset = 0;
                                SourcePage page;
                                while ((page = reader.nextPage()) != null) {
                                    int count = page.getPositionCount();
                                    int[] selected = selection.apply(count);
                                    page.selectPositions(selected, 0, selected.length);
                                    Block block = page.getBlock(0);
                                    assertThat(block.getPositionCount()).isEqualTo(selected.length);
                                    for (int position = 0; position < selected.length; position++) {
                                        Variant actual = valueAt(block, position);
                                        Variant value = expected.get(offset + selected[position]);
                                        if (paths.isEmpty()) {
                                            assertValue(actual, value);
                                        }
                                        else {
                                            for (VariantPath path : paths) {
                                                assertObservations(observe(actual, path), observe(value, path));
                                            }
                                        }
                                    }
                                    if (!name.equals("plain.parquet")) {
                                        assertThat(reader.shreddedVariantRowsReconstructed()).isEqualTo(selective ? selected.length : count);
                                    }
                                    offset += count;
                                }
                                assertThat(offset).isEqualTo(expected.size());
                            }
                            finally {
                                memory.close();
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void testTypedProbesAndReferenceFallback()
            throws IOException
    {
        List<Variant> expected = expectedRows();
        for (String name : List.of("object.parquet", "array.parquet")) {
            long typed = 0;
            long unresolved = 0;
            ShreddedVariantField field = variantField(fixture(name), PATHS);
            AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
            try (ParquetReader reader = open(fixture(name), PATHS, true, 7, memory)) {
                int offset = 0;
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    assertThat(reader.variantLeavesDecoded()).isZero();
                    VariantReadView view = reader.variantView(field);
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        for (VariantPath path : PATHS) {
                            List<VariantPathProbe> probes = view.probe(path, row);
                            if (probes.stream().anyMatch(probe -> probe.state() == UNRESOLVED)) {
                                unresolved++;
                                assertObservations(observe(view.reference(row).orElse(null), path), observe(expected.get(offset + row), path));
                            }
                            else {
                                List<Observation> observations = new ArrayList<>();
                                for (VariantPathProbe probe : probes) {
                                    if (probe.state() == TYPED_VALUE) {
                                        typed++;
                                        observations.add(new Observation(TYPED_VALUE, Optional.of(probe.value())));
                                    }
                                    else {
                                        observations.add(new Observation(probe.state(), Optional.empty()));
                                    }
                                }
                                assertObservations(observations, observe(expected.get(offset + row), path));
                            }
                        }
                    }
                    offset += page.getPositionCount();
                }
                assertThat(offset).isEqualTo(expected.size());
            }
            finally {
                memory.close();
            }
            assertThat(typed).as("typed observations for %s", name).isPositive();
            assertThat(unresolved).as("reference fallback for %s", name).isPositive();
        }
    }

    private static Path fixture(String name)
    {
        String directory = System.getProperty(FIXTURE_DIRECTORY);
        checkArgument(directory != null && !directory.isBlank(), "Set -D%s to GeoMesaVariantFixtureTest output", FIXTURE_DIRECTORY);
        Path path = Path.of(directory).resolve(name);
        checkArgument(Files.isRegularFile(path), "Missing GeoMesa writer fixture: %s", path);
        return path;
    }

    private static List<Variant> expectedRows()
            throws IOException
    {
        List<Variant> rows = new ArrayList<>();
        try (JsonParser parser = jsonFactory().createParser(fixture("expected.json").toFile())) {
            assertThat(parser.nextToken()).isEqualTo(JsonToken.START_ARRAY);
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() == JsonToken.VALUE_NULL) {
                    rows.add(null);
                }
                else {
                    assertThat(parser.currentToken()).isEqualTo(JsonToken.VALUE_STRING);
                    // Parsing source JSON does not invoke the writer, file reader or reconstruction implementation.
                    var value = VariantJsonParser.parseJson(parser.getText());
                    rows.add(Variant.from(Metadata.from(copy(value.getMetadataBuffer())), copy(value.getValueBuffer())));
                }
            }
            assertThat(parser.nextToken()).isNull();
        }
        return rows;
    }

    private static Slice copy(ByteBuffer buffer)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return wrappedBuffer(bytes);
    }

    private static ParquetReader open(Path file, Collection<VariantPath> paths, boolean selective, int batchSize, AggregatedMemoryContext memory)
            throws IOException
    {
        var options = ParquetReaderOptions.builder().withMaxReadBlockRowCount(batchSize).withShreddedVariantSelectiveRebuild(selective).build();
        var source = new TestingParquetDataSource(wrappedBuffer(Files.readAllBytes(file)), options);
        var metadata = MetadataReader.readFooter(source, Optional.empty());
        MessageType schema = metadata.getFileMetaData().getSchema();
        Field field = field(schema, paths);
        var descriptors = getDescriptors(schema, schema);
        List<RowGroupInfo> groups = new ArrayList<>();
        long offset = 0;
        for (var group : metadata.getBlocks()) {
            groups.add(new RowGroupInfo(PrunedBlockMetadata.createPrunedColumnsMetadata(group, source.getId(), descriptors), offset, Optional.empty()));
            offset += group.rowCount();
        }
        return new ParquetReader(
                Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                List.of(new Column(COLUMN, field)),
                false,
                groups,
                source,
                UTC,
                memory,
                options,
                exception -> new RuntimeException(exception),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static ShreddedVariantField variantField(Path file, Collection<VariantPath> paths)
            throws IOException
    {
        var source = new TestingParquetDataSource(wrappedBuffer(Files.readAllBytes(file)), ParquetReaderOptions.defaultOptions());
        return (ShreddedVariantField) field(MetadataReader.readFooter(source, Optional.empty()).getFileMetaData().getSchema(), paths);
    }

    private static Field field(MessageType fileSchema, Collection<VariantPath> paths)
    {
        MessageType requested = fileSchema;
        if (!paths.isEmpty()) {
            requested = new MessageType(fileSchema.getName(), List.of(ShreddedVariantPruning.prune(COLUMN, fileSchema.getType(COLUMN).asGroupType(), paths)));
        }
        return constructField(VARIANT, lookupColumnByName(getColumnIO(fileSchema, requested), COLUMN)).orElseThrow();
    }

    private static List<Variant> read(Path file, Collection<VariantPath> paths, boolean selective, int batchSize)
            throws IOException
    {
        List<Variant> rows = new ArrayList<>();
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try (ParquetReader reader = open(file, paths, selective, batchSize, memory)) {
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                Block block = page.getBlock(0);
                for (int row = 0; row < page.getPositionCount(); row++) {
                    rows.add(valueAt(block, row));
                }
            }
        }
        finally {
            memory.close();
        }
        return rows;
    }

    private static Variant valueAt(Block block, int position)
    {
        return block.isNull(position) ? null : VARIANT.getObject(block, position);
    }

    private static void assertRows(List<Variant> actual, List<Variant> expected)
    {
        assertThat(actual).hasSameSizeAs(expected);
        for (int row = 0; row < actual.size(); row++) {
            assertValue(actual.get(row), expected.get(row));
        }
    }

    static void assertValue(Variant actual, Variant expected)
    {
        assertThat(actual).isEqualTo(expected);
        if (actual == null) {
            return;
        }
        assertThat(actual.basicType()).isEqualTo(expected.basicType());
        assertThat(actual.primitiveType()).isEqualTo(expected.primitiveType());
        switch (actual.basicType()) {
            case OBJECT -> actual.objectFieldNames().forEach(name -> assertValue(actual.getObjectField(name).orElseThrow(), expected.getObjectField(name).orElseThrow()));
            case ARRAY -> IntStream.range(0, actual.getArrayLength()).forEach(index -> assertValue(actual.getArrayElement(index), expected.getArrayElement(index)));
            case PRIMITIVE -> {
                switch (actual.primitiveType()) {
                    case DECIMAL4, DECIMAL8, DECIMAL16 -> assertThat(actual.getDecimal().scale()).isEqualTo(expected.getDecimal().scale());
                    default -> {}
                }
            }
            case SHORT_STRING -> {}
        }
    }

    static void assertPath(Variant actual, Variant expected, VariantPath path)
    {
        assertObservations(observe(actual, path), observe(expected, path));
    }

    private static List<Observation> observe(Variant value, VariantPath path)
    {
        if (value == null) {
            return List.of(new Observation(SQL_NULL, Optional.empty()));
        }
        return observe(value, path.steps(), 0);
    }

    private static List<Observation> observe(Variant value, List<VariantPath.Step> steps, int index)
    {
        if (index == steps.size()) {
            return List.of(new Observation(TYPED_VALUE, Optional.of(value)));
        }
        var step = steps.get(index);
        if (step.isElement()) {
            if (value.basicType() != ARRAY) {
                return List.of(new Observation(WRONG_SHAPE, Optional.empty()));
            }
            List<Observation> values = new ArrayList<>();
            for (int element = 0; element < value.getArrayLength(); element++) {
                values.addAll(observe(value.getArrayElement(element), steps, index + 1));
            }
            return values;
        }
        if (value.basicType() != OBJECT) {
            return List.of(new Observation(WRONG_SHAPE, Optional.empty()));
        }
        Optional<Variant> child = value.getObjectField(utf8Slice(step.key().orElseThrow()));
        return child.map(variant -> observe(variant, steps, index + 1)).orElseGet(() -> List.of(new Observation(MISSING, Optional.empty())));
    }

    private static void assertObservations(List<Observation> actual, List<Observation> expected)
    {
        assertThat(actual).hasSameSizeAs(expected);
        for (int index = 0; index < actual.size(); index++) {
            assertThat(actual.get(index).state()).isEqualTo(expected.get(index).state());
            assertValue(actual.get(index).value().orElse(null), expected.get(index).value().orElse(null));
        }
    }

    private record Observation(VariantPathProbe.State state, Optional<Variant> value) {}
}
