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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.VariantPath;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.variant.Variant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.variant.Variant.ObjectField;
import org.apache.parquet.variant.VariantJsonParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.createTempDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.deleteDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.prunedSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFilePruned;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readerFileSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.rowGroupCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.shredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.typedValueCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.unshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.parquet.reader.VariantPathExtraction.describe;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Baseline for reading a Parquet VARIANT column: the same logical rows stored unshredded ({@code metadata}
 * and {@code value} only) and shredded into typed columns. The shredded read is a correctness-first
 * compatibility path (typed columns decoded by the nested readers, one variant rebuilt per row), so it is not
 * expected to beat the straight copy of the {@code value} bytes; this records by how much.
 * <p>
 * The data is generated (fixed seed), in two shapes: {@code flat} (20 string and integer fields) and
 * {@code nested} (objects three levels deep and lists of 10 objects). Each layout's schema is derived from the
 * variant types the generated rows actually have, because parquet-java's writer only fills a typed column on an
 * exact physical type match. Setup prints, per file, the number of leaf values and how many sit in typed columns.
 * {@code irregularPercent} replaces a fraction of the rows with SQL NULLs and non-object values, which the layout
 * (derived from the object rows) does not match; real data has both, and they change how the nested readers wrap
 * a batch's blocks.
 * <p>
 * {@code paths} adds pruned reads (one requested path, so only the Parquet columns along it are requested); setup checks
 * each against the whole read with {@link VariantPathExtraction}, outside the measured section. The two
 * {@code readOnePercent...} methods read under a selection keeping 1% of the rows, with selective rebuild on and off.
 * <p>
 * The class is in the {@code reader} package because it reuses the package-private writer and oracle of
 * {@link ShreddedVariantTestUtils}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = SECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = SECONDS)
@Fork(1)
public class BenchmarkShreddedVariantReader
{
    private static final int ROWS = 100_000;
    private static final long SEED = 20261110L;

    @Param({"flat", "nested"})
    public String shape;

    @Param({"unshredded", "shredded_all", "shredded_half"})
    public String layout;

    // percent of rows that are irregular: half SQL NULL, half a value that is not an object (so the root's typed_value is
    // null and the row sits in value). 0 is the data of the first baseline, row for row.
    @Param({"0", "1"})
    public int irregularPercent;

    // what is read: whole is the variant column as it is today; the others ask for one path (a pruned read, which
    // requests only the Parquet columns along it): scalar is a top-level scalar field (flat key_00, nested id), nested is
    // meta.inner.region (two object levels below the top-level field meta), list is items[*].sku (one key of the list's
    // elements), absent is a top-level key that is in no row. nested and list exist for the nested shape only.
    @Param({"whole", "scalar", "nested", "list", "absent"})
    public String paths;

    private java.nio.file.Path directory;
    private Slice fileData;
    // whether the read requests a pruned schema for requestedPaths (not for whole, which reads the file schema as written)
    private boolean pruned;
    private List<VariantPath> requestedPaths;
    private int requestedColumns;
    private long requestedBytes;

    @Setup
    public void setup()
            throws IOException
    {
        List<String> json = switch (shape) {
            case "flat" -> generateFlat(new Random(SEED));
            case "nested" -> generateNested(new Random(SEED));
            default -> throw new IllegalArgumentException(shape);
        };
        if (irregularPercent > 0) {
            json = withIrregularRows(json, irregularPercent);
        }
        Set<String> halfFields = switch (shape) {
            // every other field, in declaration order
            case "flat" -> flatFieldNames().stream().filter(name -> Integer.parseInt(name.substring(4)) % 2 == 0).collect(toImmutableSet());
            // the meta subtree (3 levels of objects and a list of 10 objects) and id; name and items stay in value
            default -> ImmutableSet.of("id", "meta");
        };

        List<org.apache.parquet.variant.Variant> rows = new ArrayList<>(ROWS);
        Node node = new Node();
        for (String row : json) {
            if (row == null) {
                rows.add(null);
                continue;
            }
            org.apache.parquet.variant.Variant variant = VariantJsonParser.parseJson(row);
            rows.add(variant);
            // the layout is derived from the object rows; an irregular row is meant not to match it
            if (isObject(variant)) {
                node.merge(variant);
            }
        }
        long leaves = 0;
        long objectLeaves = 0;
        for (org.apache.parquet.variant.Variant row : rows) {
            if (row == null) {
                continue;
            }
            long count = countLeaves(row, null, true);
            leaves += count;
            if (isObject(row)) {
                objectLeaves += count;
            }
        }

        directory = createTempDirectory();
        File file = switch (layout) {
            case "unshredded" -> writeVariantFile(directory, "file.parquet", unshredded(), rows);
            case "shredded_all" -> writeVariantFile(directory, "file.parquet", shredded(node.rootTypedValue(null)), rows);
            case "shredded_half" -> writeVariantFile(directory, "file.parquet", shredded(node.rootTypedValue(halfFields)), rows);
            default -> throw new IllegalArgumentException(layout);
        };

        long expectedTyped = switch (layout) {
            case "unshredded" -> 0;
            case "shredded_all" -> objectLeaves;
            default -> {
                long count = 0;
                for (org.apache.parquet.variant.Variant row : rows) {
                    if (row != null && isObject(row)) {
                        count += countLeaves(row, halfFields, true);
                    }
                }
                yield count;
            }
        };
        long typed = typedValueCount(file);
        if (typed != expectedTyped) {
            throw new IllegalStateException("%s/%s: %s leaf values in typed columns, expected %s of %s".formatted(shape, layout, typed, expectedTyped, leaves));
        }
        if (layout.equals("shredded_all") && typed < 0.99 * leaves) {
            throw new IllegalStateException("shredded_all is not shredded: %s of %s".formatted(typed, leaves));
        }
        if (layout.equals("unshredded") && typed != 0) {
            throw new IllegalStateException("unshredded file has typed values: " + typed);
        }

        // correctness guard, outside the measured section: the shredded read equals the unshredded read
        if (!layout.equals("unshredded")) {
            File reference = writeVariantFile(directory, "reference.parquet", unshredded(), rows);
            assertLogicallyEqual(readFile(file), readFile(reference), json);
            Files.delete(reference.toPath());
        }

        requestedPaths = pathsFor(shape, paths);
        pruned = !paths.equals("whole");
        if (pruned) {
            // contract check, outside the measured section: a pruned read gives, for the requested path, what the whole
            // read gives, for every row (VariantPathExtraction is computed with the variant SPI alone)
            List<Variant> whole = readFile(file);
            List<Variant> prunedRows = readFilePruned(file, requestedPaths);
            if (whole.size() != ROWS || prunedRows.size() != ROWS) {
                throw new IllegalStateException("%s/%s/%s: read %s and %s rows, expected %s".formatted(shape, layout, paths, whole.size(), prunedRows.size(), ROWS));
            }
            for (VariantPath path : requestedPaths) {
                for (int row = 0; row < ROWS; row++) {
                    VariantPathExtraction.Extraction expected = extract(whole.get(row), path);
                    VariantPathExtraction.Extraction actual = extract(prunedRows.get(row), path);
                    if (!same(actual, expected)) {
                        throw new IllegalStateException("%s/%s/%s: row %s path %s: pruned read gives %s, whole read gives %s".formatted(shape, layout, paths, row, path, describe(actual), describe(expected)));
                    }
                }
            }
        }
        countRequestedColumns(file);

        printStats(file, leaves, typed);
        fileData = Slices.wrappedBuffer(Files.readAllBytes(file.toPath()));
    }

    @TearDown
    public void tearDown()
    {
        deleteDirectory(directory);
    }

    @Benchmark
    public long readVariantColumn()
            throws IOException
    {
        return read(ParquetReaderOptions.defaultOptions(), false);
    }

    // The read under a filter that keeps 1% of the rows (position 37 of every 100 in each batch), selective rebuild on
    @Benchmark
    public long readOnePercentSelectiveRebuildOn()
            throws IOException
    {
        return read(ParquetReaderOptions.builder().withShreddedVariantSelectiveRebuild(true).build(), true);
    }

    // The same, selective rebuild off: every row of the batch is rebuilt, then the selection drops 99%
    @Benchmark
    public long readOnePercentSelectiveRebuildOff()
            throws IOException
    {
        return read(ParquetReaderOptions.builder().withShreddedVariantSelectiveRebuild(false).build(), true);
    }

    private long read(ParquetReaderOptions options, boolean selectOnePercent)
            throws IOException
    {
        ParquetDataSource dataSource = new TestingParquetDataSource(fileData, options);
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
        MessageType requested = pruned ? prunedSchema(metadata.getFileMetaData().getSchema(), requestedPaths) : metadata.getFileMetaData().getSchema();
        long checksum = 0;
        try (ParquetReader reader = createParquetReader(dataSource, metadata, requested, options, newSimpleAggregatedMemoryContext(), ImmutableList.of(VARIANT), ImmutableList.of("v"), TupleDomain.all(), false, UTC)) {
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                if (selectOnePercent) {
                    int rows = page.getPositionCount();
                    int count = rows > 37 ? (rows - 38) / 100 + 1 : 0;
                    int[] positions = new int[count];
                    for (int i = 0; i < count; i++) {
                        positions[i] = 37 + 100 * i;
                    }
                    page.selectPositions(positions, 0, count);
                }
                Block block = page.getBlock(0);
                checksum += block.getPositionCount() + block.getSizeInBytes();
            }
        }
        return checksum;
    }

    private static List<VariantPath> pathsFor(String shape, String paths)
    {
        return switch (paths) {
            case "whole" -> ImmutableList.of();
            case "scalar" -> ImmutableList.of(VariantPath.keys(shape.equals("flat") ? "key_00" : "id"));
            case "nested" -> ImmutableList.of(VariantPath.keys("meta", "inner", "region"));
            case "list" -> ImmutableList.of(VariantPath.of(VariantPath.Step.key("items"), VariantPath.Step.element(), VariantPath.Step.key("sku")));
            case "absent" -> ImmutableList.of(VariantPath.keys("no_such_key"));
            default -> throw new IllegalArgumentException(paths);
        };
    }

    // The Parquet leaf columns of v the read requests (all of them for whole) and their chunk sizes in the file
    private void countRequestedColumns(File file)
    {
        MessageType fileSchema = readerFileSchema(file);
        MessageType requested = pruned ? prunedSchema(fileSchema, requestedPaths) : fileSchema;
        Set<List<String>> requestedLeaves = new HashSet<>();
        for (String[] path : requested.getPaths()) {
            if (path[0].equals("v")) {
                requestedLeaves.add(List.of(path));
            }
        }
        long bytes = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), new Configuration()))) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData chunk : block.getColumns()) {
                    if (requestedLeaves.contains(List.of(chunk.getPath().toArray()))) {
                        bytes += chunk.getTotalSize();
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        requestedColumns = requestedLeaves.size();
        requestedBytes = bytes;
        System.out.printf("REQUESTED-COLUMNS shape=%s layout=%s irregularPercent=%d paths=%s columns=%d chunkBytes=%d%n", shape, layout, irregularPercent, paths, requestedColumns, requestedBytes);
    }

    private void printStats(File file, long leaves, long typed)
    {
        long metadataBytes = 0;
        long valueBytes = 0;
        long topLevelValueBytes = 0;
        long typedBytes = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), new Configuration()))) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData chunk : block.getColumns()) {
                    String[] path = chunk.getPath().toArray();
                    String leaf = path[path.length - 1];
                    long size = chunk.getTotalSize();
                    if (leaf.equals("metadata")) {
                        metadataBytes += size;
                    }
                    else if (leaf.equals("value")) {
                        valueBytes += size;
                        if (path.length == 2) {
                            topLevelValueBytes += size;
                        }
                    }
                    else {
                        typedBytes += size;
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        System.out.printf(
                "FILE-STATS shape=%s layout=%s irregularPercent=%d rows=%d fileBytes=%d rowGroups=%d leafValues=%d typedLeafValues=%d typedFraction=%.4f metadataChunkBytes=%d valueChunkBytesAllLevels=%d topLevelValueChunkBytes=%d typedChunkBytes=%d%n",
                shape,
                layout,
                irregularPercent,
                ROWS,
                file.length(),
                rowGroupCount(file),
                leaves,
                typed,
                (double) typed / leaves,
                metadataBytes,
                valueBytes,
                topLevelValueBytes,
                typedBytes);
    }

    /**
     * Replaces one row in every {@code 100 / percent} with an irregular one, alternating a SQL NULL and a string; the
     * other rows are unchanged, so they are the same as at 0.
     */
    private static List<String> withIrregularRows(List<String> json, int percent)
    {
        int period = 100 / percent;
        List<String> rows = new ArrayList<>(json.size());
        for (int row = 0; row < json.size(); row++) {
            if (row % period == 0) {
                rows.add((row / period) % 2 == 0 ? null : "\"not an object %s\"".formatted(row));
            }
            else {
                rows.add(json.get(row));
            }
        }
        return rows;
    }

    private static boolean isObject(org.apache.parquet.variant.Variant variant)
    {
        return variant.getType() == org.apache.parquet.variant.Variant.Type.OBJECT;
    }

    private static List<String> flatFieldNames()
    {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            names.add("key_%02d".formatted(i));
        }
        return names;
    }

    // flat: key_00..key_19; fields with i % 5 >= 3 are integers (8 of them), the rest strings (12).
    // Even-numbered strings are random 8-16 letter words, odd-numbered ones come from a pool of 100.
    private static List<String> generateFlat(Random random)
    {
        String[] pool = new String[100];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = randomWord(random, 6, 12);
        }
        List<String> rows = new ArrayList<>(ROWS);
        for (int row = 0; row < ROWS; row++) {
            StringBuilder json = new StringBuilder("{");
            for (int i = 0; i < 20; i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append("\"key_%02d\":".formatted(i));
                if (i % 5 >= 3) {
                    json.append(randomInt(random));
                }
                else if (i % 2 == 0) {
                    json.append('"').append(randomWord(random, 8, 16)).append('"');
                }
                else {
                    json.append('"').append(pool[random.nextInt(pool.length)]).append('"');
                }
            }
            rows.add(json.append('}').toString());
        }
        return rows;
    }

    // nested: {id, name, meta:{source, version, inner:{region, score, deep:{label, count, active}, events:[10 x {kind, at}]}}, items:[10 x {sku, qty, price, label}]}
    private static List<String> generateNested(Random random)
    {
        String[] kinds = {"create", "update", "delete", "read", "sync", "login", "logout", "export"};
        String[] regions = {"us-east", "us-west", "eu-west", "eu-north", "ap-south", "ap-east"};
        List<String> rows = new ArrayList<>(ROWS);
        for (int row = 0; row < ROWS; row++) {
            StringBuilder json = new StringBuilder();
            json.append("{\"id\":").append(randomInt(random));
            json.append(",\"name\":\"").append(randomWord(random, 8, 20)).append('"');
            json.append(",\"meta\":{\"source\":\"").append(randomWord(random, 5, 10)).append('"');
            json.append(",\"version\":").append(randomInt(random));
            json.append(",\"inner\":{\"region\":\"").append(regions[random.nextInt(regions.length)]).append('"');
            json.append(",\"score\":").append(randomInt(random));
            json.append(",\"deep\":{\"label\":\"").append(randomWord(random, 6, 14)).append('"');
            json.append(",\"count\":").append(randomInt(random));
            json.append(",\"active\":").append(random.nextBoolean()).append('}');
            json.append(",\"events\":[");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append("{\"kind\":\"").append(kinds[random.nextInt(kinds.length)]).append('"');
                json.append(",\"at\":").append(randomInt(random)).append('}');
            }
            json.append("]}},\"items\":[");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append("{\"sku\":\"").append(randomWord(random, 8, 12)).append('"');
                json.append(",\"qty\":").append(randomInt(random));
                json.append(",\"price\":").append(randomInt(random));
                json.append(",\"label\":\"").append(randomWord(random, 6, 18)).append("\"}");
            }
            rows.add(json.append("]}").toString());
        }
        return rows;
    }

    // always a 32-bit integer in the variant encoding (the JSON parser picks the narrowest width)
    private static int randomInt(Random random)
    {
        return 100_000 + random.nextInt(1_900_000_000);
    }

    private static String randomWord(Random random, int minLength, int maxLength)
    {
        int length = minLength + random.nextInt(maxLength - minLength + 1);
        StringBuilder word = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            word.append((char) ('a' + random.nextInt(26)));
        }
        return word.toString();
    }

    /**
     * Counts primitive values in the variant. When {@code topLevelFields} is not null, only values under those
     * top-level object fields count.
     */
    private static long countLeaves(org.apache.parquet.variant.Variant variant, Set<String> topLevelFields, boolean top)
    {
        switch (variant.getType()) {
            case OBJECT -> {
                long count = 0;
                for (int i = 0; i < variant.numObjectElements(); i++) {
                    ObjectField field = variant.getFieldAtIndex(i);
                    if (top && topLevelFields != null && !topLevelFields.contains(field.key)) {
                        continue;
                    }
                    count += countLeaves(field.value, null, false);
                }
                return count;
            }
            case ARRAY -> {
                long count = 0;
                for (int i = 0; i < variant.numArrayElements(); i++) {
                    count += countLeaves(variant.getElementAtIndex(i), null, false);
                }
                return count;
            }
            default -> {
                return 1;
            }
        }
    }

    /**
     * The shape of the data with one physical type per path, merged over all rows. A path whose rows disagree
     * on the type is marked mixed and gets no typed column.
     */
    private static final class Node
    {
        private enum Kind
        {
            UNSET, OBJECT, ARRAY, PRIMITIVE
        }

        private Kind kind = Kind.UNSET;
        private org.apache.parquet.variant.Variant.Type primitiveType;
        private boolean mixed;
        private final Map<String, Node> fields = new LinkedHashMap<>();
        private Node element;

        void merge(org.apache.parquet.variant.Variant variant)
        {
            org.apache.parquet.variant.Variant.Type type = variant.getType();
            Kind actual = switch (type) {
                case OBJECT -> Kind.OBJECT;
                case ARRAY -> Kind.ARRAY;
                default -> Kind.PRIMITIVE;
            };
            if (kind == Kind.UNSET) {
                kind = actual;
                primitiveType = actual == Kind.PRIMITIVE ? type : null;
            }
            else if (kind != actual || (actual == Kind.PRIMITIVE && primitiveType != type)) {
                mixed = true;
                return;
            }
            if (actual == Kind.OBJECT) {
                for (int i = 0; i < variant.numObjectElements(); i++) {
                    ObjectField field = variant.getFieldAtIndex(i);
                    fields.computeIfAbsent(field.key, _ -> new Node()).merge(field.value);
                }
            }
            else if (actual == Kind.ARRAY) {
                if (element == null) {
                    element = new Node();
                }
                for (int i = 0; i < variant.numArrayElements(); i++) {
                    element.merge(variant.getElementAtIndex(i));
                }
            }
        }

        /**
         * The root's {@code typed_value} declaration, shredding only the named top-level fields (all when null).
         */
        String rootTypedValue(Set<String> topLevelFields)
        {
            if (mixed || kind != Kind.OBJECT) {
                throw new IllegalStateException("The rows are not uniformly objects");
            }
            return typedValue(topLevelFields);
        }

        private String typedValue(Set<String> objectFields)
        {
            if (mixed) {
                return "";
            }
            switch (kind) {
                case PRIMITIVE -> {
                    return switch (primitiveType) {
                        case STRING -> "optional binary typed_value (STRING);";
                        case INT -> "optional int32 typed_value;";
                        case LONG -> "optional int64 typed_value;";
                        case BOOLEAN -> "optional boolean typed_value;";
                        default -> throw new IllegalStateException("No typed column defined for " + primitiveType);
                    };
                }
                case OBJECT -> {
                    StringBuilder group = new StringBuilder("optional group typed_value {\n");
                    for (Map.Entry<String, Node> field : fields.entrySet()) {
                        if (objectFields != null && !objectFields.contains(field.getKey())) {
                            continue;
                        }
                        group.append("required group %s { optional binary value; %s }\n".formatted(field.getKey(), field.getValue().typedValue(null)));
                    }
                    return group.append("}").toString();
                }
                case ARRAY -> {
                    return "optional group typed_value (LIST) { repeated group list { required group element { optional binary value; %s } } }".formatted(element.typedValue(null));
                }
                default -> throw new IllegalStateException("No values");
            }
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkShreddedVariantReader.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
