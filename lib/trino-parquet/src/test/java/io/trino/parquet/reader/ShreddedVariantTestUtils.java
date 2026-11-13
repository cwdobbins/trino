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
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ShreddedVariantPruning;
import io.trino.parquet.VariantPath;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import io.trino.type.VariantOperators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.variant.VariantJsonParser;
import org.apache.parquet.variant.VariantValueWriter;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Writes variant rows with parquet-java's own variant writer, in a shredded or unshredded layout,
 * and reads them back through Trino's Parquet reader.
 */
final class ShreddedVariantTestUtils
{
    private static final String COLUMN = "v";

    private ShreddedVariantTestUtils() {}

    static MessageType unshredded()
    {
        return MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    required binary value;
                  }
                }
                """);
    }

    static MessageType shredded(String typedValueSchema)
    {
        return MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    %s
                  }
                }
                """.formatted(typedValueSchema));
    }

    /**
     * Writes the rows (a null entry is a SQL NULL row) as one file whose single column {@code v}
     * has the layout of the variant group in {@code fileSchema}, and reads it back as VARIANT.
     */
    static List<Variant> writeAndRead(MessageType fileSchema, List<String> jsonRows)
    {
        java.nio.file.Path directory = createTempDirectory();
        try {
            return readFile(writeFile(directory, "file.parquet", fileSchema, jsonRows));
        }
        finally {
            deleteDirectory(directory);
        }
    }

    /**
     * As {@link #writeAndRead(MessageType, List)}, for rows built with parquet-java's variant builder
     * (types the JSON parser cannot produce); a null entry is a SQL NULL row.
     */
    static List<Variant> writeAndReadVariants(MessageType fileSchema, List<org.apache.parquet.variant.Variant> rows)
    {
        java.nio.file.Path directory = createTempDirectory();
        try {
            return readFile(writeVariantFile(directory, "file.parquet", fileSchema, rows));
        }
        finally {
            deleteDirectory(directory);
        }
    }

    static File writeFile(java.nio.file.Path directory, String name, MessageType fileSchema, List<String> jsonRows)
    {
        List<org.apache.parquet.variant.Variant> rows = new ArrayList<>();
        for (String json : jsonRows) {
            try {
                rows.add(json == null ? null : VariantJsonParser.parseJson(json));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return writeVariantFile(directory, name, fileSchema, rows);
    }

    static File writeVariantFile(java.nio.file.Path directory, String name, MessageType fileSchema, List<org.apache.parquet.variant.Variant> rows)
    {
        return writeVariantFile(directory, name, fileSchema, rows, 0, 0);
    }

    /**
     * As {@link #writeVariantFile(java.nio.file.Path, String, MessageType, List)}, with the writer's row group
     * and page sizes in bytes (a size of 0 keeps the writer's default).
     */
    static File writeVariantFile(java.nio.file.Path directory, String name, MessageType fileSchema, List<org.apache.parquet.variant.Variant> rows, long rowGroupSize, int pageSize)
    {
        File file = directory.resolve(name).toFile();
        GroupType variantGroup = fileSchema.getType(COLUMN).asGroupType();
        VariantWriterBuilder builder = new VariantWriterBuilder(new Path(file.toURI()), fileSchema, variantGroup);
        if (rowGroupSize > 0) {
            builder.withRowGroupSize(rowGroupSize);
        }
        if (pageSize > 0) {
            builder.withPageSize(pageSize);
        }
        try (ParquetWriter<org.apache.parquet.variant.Variant> writer = builder.build()) {
            for (org.apache.parquet.variant.Variant row : rows) {
                writer.write(row);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    /**
     * Writes rows built directly as groups (for states a writer will not emit).
     */
    static File writeGroupFile(java.nio.file.Path directory, String name, MessageType fileSchema, List<? extends Group> rows)
    {
        File file = directory.resolve(name).toFile();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
                .withType(fileSchema)
                .withConf(new Configuration())
                .build()) {
            for (Group row : rows) {
                writer.write(row);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    /**
     * Writes records by calling each entry with the record consumer between startMessage and endMessage;
     * for schemas where the variant sits inside a row, array or map.
     */
    static File writeRecords(java.nio.file.Path directory, String name, MessageType fileSchema, List<Consumer<RecordConsumer>> records)
    {
        File file = directory.resolve(name).toFile();
        try (ParquetWriter<Consumer<RecordConsumer>> writer = new RecordWriterBuilder(new Path(file.toURI()), fileSchema).build()) {
            for (Consumer<RecordConsumer> record : records) {
                writer.write(record);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    static java.nio.file.Path createTempDirectory()
    {
        try {
            return Files.createTempDirectory("shredded-variant");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void deleteDirectory(java.nio.file.Path directory)
    {
        try {
            MoreFiles.deleteRecursively(directory, RecursiveDeleteOption.ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Number of non-null values stored in leaf columns named {@code typed_value} (at any depth),
     * read from the file's column chunk statistics, or from the column data when a chunk has none.
     */
    static long typedValueCount(File parquetFile)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFile.toURI()), new Configuration()))) {
            long count = 0;
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData chunk : block.getColumns()) {
                    if (!isTypedValue(chunk.getPath().toArray())) {
                        continue;
                    }
                    if (chunk.getStatistics() != null && chunk.getStatistics().isNumNullsSet()) {
                        count += chunk.getValueCount() - chunk.getStatistics().getNumNulls();
                    }
                    else {
                        count += countNonNullValues(parquetFile, chunk.getPath().toArray());
                    }
                }
            }
            return count;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long countNonNullValues(File parquetFile, String[] path)
            throws IOException
    {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFile.toURI()), new Configuration()))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore rowGroup;
            while ((rowGroup = reader.readNextRowGroup()) != null) {
                RecordReader<Group> recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                for (long row = 0; row < rowGroup.getRowCount(); row++) {
                    count += countTypedValues(recordReader.read(), path, 0);
                }
            }
        }
        return count;
    }

    private static long countTypedValues(Group group, String[] path, int depth)
    {
        int index = group.getType().getFieldIndex(path[depth]);
        int repetitions = group.getFieldRepetitionCount(index);
        if (depth == path.length - 1) {
            return repetitions;
        }
        long count = 0;
        for (int i = 0; i < repetitions; i++) {
            count += countTypedValues(group.getGroup(index, i), path, depth + 1);
        }
        return count;
    }

    private static boolean isTypedValue(String[] path)
    {
        return path[path.length - 1].equals("typed_value");
    }

    private static boolean hasTypedValueColumn(File parquetFile)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFile.toURI()), new Configuration()))) {
            return reader.getFooter().getFileMetaData().getSchema().getColumns().stream()
                    .anyMatch(column -> isTypedValue(column.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads column {@code v} of a Parquet file as VARIANT; a SQL NULL row is a null entry.
     */
    static List<Variant> readFile(File parquetFile)
    {
        return readFile(parquetFile, ParquetReaderOptions.defaultOptions().getMaxReadBlockRowCount());
    }

    /**
     * As {@link #readFile(File)}, with the reader's batch size capped at {@code maxBatchRows}.
     */
    static List<Variant> readFile(File parquetFile, int maxBatchRows)
    {
        return readColumn(parquetFile, COLUMN, VARIANT, maxBatchRows, ShreddedVariantTestUtils::variantAt);
    }

    /**
     * Reads column {@code v} as VARIANT through the requested schema a pruned read uses: the file schema with the
     * variant group replaced by {@link ShreddedVariantPruning#prune} of it for {@code paths}. Otherwise the read is the
     * one {@link #readFile(File)} makes.
     */
    static List<Variant> readFilePruned(File parquetFile, Collection<VariantPath> paths)
    {
        return readFilePruned(parquetFile, paths, ParquetReaderOptions.defaultOptions().getMaxReadBlockRowCount());
    }

    /**
     * As {@link #readFilePruned(File, Collection)}, with the reader's batch size capped at {@code maxBatchRows}.
     */
    static List<Variant> readFilePruned(File parquetFile, Collection<VariantPath> paths, int maxBatchRows)
    {
        return readColumn(parquetFile, COLUMN, VARIANT, maxBatchRows, fileSchema -> prunedSchema(fileSchema, paths), ShreddedVariantTestUtils::variantAt);
    }

    /**
     * The file schema with column {@code v} replaced by its pruned group for {@code paths}.
     */
    static MessageType prunedSchema(MessageType fileSchema, Collection<VariantPath> paths)
    {
        ImmutableList.Builder<org.apache.parquet.schema.Type> fields = ImmutableList.builder();
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            fields.add(field.getName().equals(COLUMN) ? ShreddedVariantPruning.prune(COLUMN, field.asGroupType(), paths) : field);
        }
        return new MessageType(fileSchema.getName(), fields.build());
    }

    /**
     * The file schema as the reader sees it (names lowercased by the footer reader except below a VARIANT group).
     */
    static MessageType readerFileSchema(File parquetFile)
    {
        try {
            ParquetDataSource dataSource = new TestingParquetDataSource(
                    Slices.wrappedBuffer(Files.readAllBytes(parquetFile.toPath())),
                    ParquetReaderOptions.defaultOptions());
            return MetadataReader.readFooter(dataSource, Optional.empty()).getFileMetaData().getSchema();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Variant variantAt(Block block, int position)
    {
        return block.isNull(position) ? null : (Variant) VARIANT.getObject(block, position);
    }

    /**
     * Reads one column as the given Trino type, converting each position with {@code reader}.
     */
    static <T> List<T> readColumn(File parquetFile, String column, Type type, int maxBatchRows, BiFunction<Block, Integer, T> reader)
    {
        return readColumn(parquetFile, column, type, maxBatchRows, fileSchema -> fileSchema, reader);
    }

    // The one read path: the requested schema is derived from the file schema, which reads the file as written
    private static <T> List<T> readColumn(File parquetFile, String column, Type type, int maxBatchRows, UnaryOperator<MessageType> requestedSchema, BiFunction<Block, Integer, T> reader)
    {
        try {
            ParquetReaderOptions options = ParquetReaderOptions.builder().withMaxReadBlockRowCount(maxBatchRows).build();
            ParquetDataSource dataSource = new TestingParquetDataSource(
                    Slices.wrappedBuffer(Files.readAllBytes(parquetFile.toPath())),
                    options);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            MessageType requested = requestedSchema.apply(parquetMetadata.getFileMetaData().getSchema());
            List<T> values = new ArrayList<>();
            try (ParquetReader parquetReader = createParquetReader(dataSource, parquetMetadata, requested, options, newSimpleAggregatedMemoryContext(), ImmutableList.of(type), ImmutableList.of(column), TupleDomain.all(), false, UTC)) {
                SourcePage page;
                while ((page = parquetReader.nextPage()) != null) {
                    Block block = page.getBlock(0);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        values.add(reader.apply(block, position));
                    }
                }
            }
            return values;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Total number of data pages of the leaf column at {@code path}, over all row groups.
     */
    static int dataPageCount(File parquetFile, String... path)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFile.toURI()), new Configuration()))) {
            ColumnDescriptor descriptor = reader.getFooter().getFileMetaData().getSchema().getColumnDescription(path);
            int pages = 0;
            PageReadStore rowGroup;
            while ((rowGroup = reader.readNextRowGroup()) != null) {
                PageReader pageReader = rowGroup.getPageReader(descriptor);
                while (pageReader.readPage() != null) {
                    pages++;
                }
            }
            return pages;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Number of row groups in the file's footer.
     */
    static int rowGroupCount(File parquetFile)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFile.toURI()), new Configuration()))) {
            return reader.getFooter().getBlocks().size();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads the rows through the given shredded layout and through the unshredded layout and
     * asserts each pair is logically equal: both SQL NULL, or equal as Variant and as JSON text.
     * Also asserts that the shredded file really holds typed values.
     */
    static void assertSameAsUnshredded(MessageType shreddedSchema, List<String> jsonRows)
    {
        assertSameAsUnshredded(shreddedSchema, jsonRows, true);
    }

    /**
     * As {@link #assertSameAsUnshredded(MessageType, List)}; with {@code expectTypedValues} false,
     * asserts instead that the shredded file holds no typed value (every row falls back to {@code value}).
     */
    static void assertSameAsUnshredded(MessageType shreddedSchema, List<String> jsonRows, boolean expectTypedValues)
    {
        java.nio.file.Path directory = createTempDirectory();
        try {
            File shreddedFile = writeFile(directory, "shredded.parquet", shreddedSchema, jsonRows);
            File plainFile = writeFile(directory, "plain.parquet", unshredded(), jsonRows);

            long typedValues = typedValueCount(shreddedFile);
            if (expectTypedValues) {
                assertThat(typedValues)
                        .as("the file was expected to be shredded but no typed values were written")
                        .isGreaterThan(0);
            }
            else {
                assertThat(typedValues)
                        .as("the file was expected to hold no typed values")
                        .isEqualTo(0);
            }
            assertThat(hasTypedValueColumn(plainFile)).as("the unshredded file has a typed_value column").isFalse();

            assertLogicallyEqual(readFile(shreddedFile), readFile(plainFile), jsonRows);
        }
        finally {
            deleteDirectory(directory);
        }
    }

    /**
     * As {@link #assertSameAsUnshredded(MessageType, List)}, for rows built with parquet-java's variant
     * builder, and asserting that exactly {@code expectedTypedValues} values landed in {@code typed_value}.
     * Returns what the shredded file reads as.
     */
    static List<Variant> assertVariantsSameAsUnshredded(MessageType shreddedSchema, List<org.apache.parquet.variant.Variant> rows, long expectedTypedValues)
    {
        java.nio.file.Path directory = createTempDirectory();
        try {
            File shreddedFile = writeVariantFile(directory, "shredded.parquet", shreddedSchema, rows);
            File plainFile = writeVariantFile(directory, "plain.parquet", unshredded(), rows);

            assertThat(typedValueCount(shreddedFile)).as("typed values written").isEqualTo(expectedTypedValues);
            assertThat(hasTypedValueColumn(plainFile)).as("the unshredded file has a typed_value column").isFalse();

            List<Variant> actual = readFile(shreddedFile);
            assertLogicallyEqual(actual, readFile(plainFile), rows.stream().map(row -> row == null ? "SQL NULL" : row.getType().name()).toList());
            return actual;
        }
        finally {
            deleteDirectory(directory);
        }
    }

    /**
     * Asserts that each pair of rows is logically equal: both SQL NULL, or equal as Variant and as JSON text.
     */
    static void assertLogicallyEqual(List<Variant> actual, List<Variant> expected, List<String> jsonRows)
    {
        assertThat(actual).as("actual row count").hasSameSizeAs(jsonRows);
        assertThat(expected).as("expected row count").hasSameSizeAs(jsonRows);
        for (int row = 0; row < jsonRows.size(); row++) {
            String description = "row %s: %s".formatted(row, jsonRows.get(row));
            Variant actualValue = actual.get(row);
            Variant expectedValue = expected.get(row);
            if (actualValue == null || expectedValue == null) {
                assertThat(actualValue).as(description).isNull();
                assertThat(expectedValue).as(description).isNull();
                continue;
            }
            assertThat(actualValue).as(description).isEqualTo(expectedValue);
            assertThat(toJson(actualValue)).as(description + " (JSON text)").isEqualTo(toJson(expectedValue));
        }
    }

    /**
     * Asserts the call fails with a NOT_SUPPORTED TrinoException that names the Parquet column path
     * of the offending node in the shredded-layout message.
     */
    static void assertUnsupportedLayout(ThrowingCallable call, String columnPath)
    {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Unsupported shredded VARIANT layout in column " + columnPath + ":");
    }

    static String toJson(Variant variant)
    {
        return VariantOperators.castToJson(variant).rawText().toStringUtf8();
    }

    private static final class RecordWriterBuilder
            extends ParquetWriter.Builder<Consumer<RecordConsumer>, RecordWriterBuilder>
    {
        private final MessageType schema;

        private RecordWriterBuilder(Path path, MessageType schema)
        {
            super(path);
            this.schema = schema;
        }

        @Override
        protected RecordWriterBuilder self()
        {
            return this;
        }

        @Override
        protected WriteSupport<Consumer<RecordConsumer>> getWriteSupport(Configuration conf)
        {
            return new WriteSupport<>()
            {
                private RecordConsumer consumer;

                @Override
                public WriteContext init(Configuration configuration)
                {
                    return new WriteContext(schema, new HashMap<>());
                }

                @Override
                public void prepareForWrite(RecordConsumer recordConsumer)
                {
                    this.consumer = recordConsumer;
                }

                @Override
                public void write(Consumer<RecordConsumer> record)
                {
                    consumer.startMessage();
                    record.accept(consumer);
                    consumer.endMessage();
                }
            };
        }
    }

    private static final class VariantWriterBuilder
            extends ParquetWriter.Builder<org.apache.parquet.variant.Variant, VariantWriterBuilder>
    {
        private final MessageType schema;
        private final GroupType variantGroup;

        private VariantWriterBuilder(Path path, MessageType schema, GroupType variantGroup)
        {
            super(path);
            this.schema = schema;
            this.variantGroup = variantGroup;
        }

        @Override
        protected VariantWriterBuilder self()
        {
            return this;
        }

        @Override
        protected WriteSupport<org.apache.parquet.variant.Variant> getWriteSupport(Configuration conf)
        {
            return new VariantWriteSupport(schema, variantGroup);
        }
    }

    private static final class VariantWriteSupport
            extends WriteSupport<org.apache.parquet.variant.Variant>
    {
        private final MessageType schema;
        private final GroupType variantGroup;
        private RecordConsumer consumer;
        private int row;

        private VariantWriteSupport(MessageType schema, GroupType variantGroup)
        {
            this.schema = schema;
            this.variantGroup = variantGroup;
        }

        @Override
        public WriteContext init(Configuration configuration)
        {
            return new WriteContext(schema, new HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer)
        {
            this.consumer = recordConsumer;
        }

        @Override
        public void write(org.apache.parquet.variant.Variant variant)
        {
            consumer.startMessage();
            // other top-level columns are required int32 or int64 columns holding the row's index, as an int32 or negated
            for (int index = 0; index < schema.getFieldCount(); index++) {
                org.apache.parquet.schema.Type field = schema.getType(index);
                if (field.getName().equals(COLUMN)) {
                    if (variant != null) {
                        consumer.startField(COLUMN, index);
                        VariantValueWriter.write(consumer, variantGroup, variant);
                        consumer.endField(COLUMN, index);
                    }
                    continue;
                }
                consumer.startField(field.getName(), index);
                switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                    case INT32 -> consumer.addInteger(row);
                    case INT64 -> consumer.addLong(-(long) row);
                    default -> throw new IllegalArgumentException("unsupported column " + field);
                }
                consumer.endField(field.getName(), index);
            }
            consumer.endMessage();
            row++;
        }
    }
}
