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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slices;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.parquet.VariantPath;
import io.trino.parquet.VariantPath.Step;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.VariantBatchAccess;
import io.trino.parquet.reader.VariantPathExtraction.Extraction;
import io.trino.parquet.reader.VariantPathProbe;
import io.trino.parquet.reader.VariantPredicateEvaluator;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.variant.Variant;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.TypeDeserializer;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.VariantShreddingFunction;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.reader.VariantPathExtraction.describe;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Reads a VARIANT column through {@link IcebergPageSourceProvider} with a path set on its handle. The data files are written by
 * Iceberg's own Parquet writer, and every file meant to be shredded is checked from its footer to hold values in
 * {@code typed_value} columns. The contract: for every requested path and row, extracting the path from the pruned variant gives
 * the same observation as from the whole variant.
 */
final class TestIcebergPrunedVariantRead
{
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "v", Types.VariantType.get()));

    private static final IcebergColumnHandle ID = IcebergColumnHandle.required(primitiveColumnIdentity(1, "id")).columnType(INTEGER).build();
    private static final IcebergColumnHandle V = IcebergColumnHandle.optional(primitiveColumnIdentity(2, "v")).columnType(VARIANT).build();

    private static final VariantPath STATUS = VariantPath.keys("status");
    private static final VariantPath N = VariantPath.keys("n");
    private static final VariantPath COUNTRY = VariantPath.keys("user", "geo", "country");
    private static final VariantPath SKU = VariantPath.of(Step.key("items"), Step.element(), Step.key("sku"));
    private static final VariantPath ROOT = VariantPath.of();

    private static final List<Object> ROWS = Arrays.asList(
            obj("status", "ok", "n", 1, "user", obj("geo", obj("country", "FR", "city", "Paris")), "items", List.of(obj("sku", "a", "qty", 2), obj("sku", "b")), "extra", "x"),
            // status and n of a type that is not the shredded one, geo not an object, items with odd elements
            obj("status", 5, "n", "two", "user", obj("geo", "nowhere"), "items", List.of(obj("qty", 1), "scalar", obj("sku", 7))),
            // status and geo missing, an empty list
            obj("user", obj("name", "u"), "items", List.of()),
            // a variant null, a user and items of the wrong shape
            obj("status", Variants.ofNull(), "user", "flat", "items", "not a list"),
            null,
            Variants.of("a plain string"),
            Variants.ofNull(),
            obj(),
            List.of(1, 2),
            obj("status", "late", "n", 9, "user", obj("geo", obj("country", "DE")), "items", List.of(obj("sku", "c", "qty", 3))));

    // status, n, user.geo.country and items[*].{sku,qty}
    private static final VariantShreddingFunction FULL_SHREDDING = objectShredding(
            field("status", string()),
            field("n", int32()),
            field("user", objectShredding(field("geo", objectShredding(field("country", string())).apply(null, null))).apply(null, null)),
            field("items", list(org.apache.parquet.schema.Types.requiredGroup()
                    .optional(BINARY).named("value")
                    .addField(objectShredding(field("sku", string()), field("qty", int32())).apply(null, null))
                    .named("element"))));

    @TempDir
    Path directory;

    @Test
    void testHandleRoundTripsThroughJson()
    {
        JsonCodec<IcebergColumnHandle> codec = codec();
        IcebergColumnHandle pruned = V.withVariantPaths(ImmutableSet.of(SKU, STATUS, VariantPath.keys("status", "deeper"), VariantPath.keys("we'ird", "va\\lue")));
        assertThat(pruned.getVariantPaths()).contains(ImmutableSet.of(SKU, STATUS, VariantPath.keys("we'ird", "va\\lue")));

        IcebergColumnHandle roundTripped = codec.fromJson(codec.toJson(pruned));
        assertThat(roundTripped).isEqualTo(pruned);
        assertThat(roundTripped.hashCode()).isEqualTo(pruned.hashCode());
        assertThat(roundTripped.getVariantPaths()).isEqualTo(pruned.getVariantPaths());
        assertThat(roundTripped.getQualifiedName()).isEqualTo("v");
        assertThat(roundTripped.getId()).isEqualTo(2);

        // a handle with no path set keeps today's JSON form, and JSON without the property reads as no path set
        String wholeJson = codec.toJson(V);
        assertThat(wholeJson).doesNotContain("variantPaths");
        assertThat(codec.fromJson(wholeJson)).isEqualTo(V);
        assertThat(codec.fromJson(wholeJson).getVariantPaths()).isEmpty();

        // the path set takes part in equality, hashing and toString
        assertThat(pruned).isNotEqualTo(V);
        assertThat(pruned.hashCode()).isNotEqualTo(V.hashCode());
        assertThat(V.withVariantPaths(ImmutableSet.of(N))).isNotEqualTo(V.withVariantPaths(ImmutableSet.of(STATUS)));
        assertThat(V.withVariantPaths(ImmutableSet.of(STATUS, VariantPath.keys("status", "x")))).isEqualTo(V.withVariantPaths(ImmutableSet.of(STATUS)));
        assertThat(pruned.toString()).isNotEqualTo(V.toString()).contains("$['status']");
        assertThat(V.toString()).isEqualTo("2:v:variant");

        // the base column carries no path set, and the existing accessors are unchanged
        assertThat(pruned.getBaseColumn()).isEqualTo(V);
        assertThat(pruned.getBaseColumn().getVariantPaths()).isEmpty();
        assertThat(pruned.isBaseColumn()).isTrue();
        assertThat(pruned.getPath()).isEmpty();
        assertThat(new IcebergColumnHandle.Builder(pruned).build()).isEqualTo(pruned);
        assertThat(new IcebergColumnHandle.Builder(pruned).nullable(false).build().getVariantPaths()).isEqualTo(pruned.getVariantPaths());
        // changing what column the builder describes clears the path set rather than failing
        assertThat(new IcebergColumnHandle.Builder(pruned).columnType(VARIANT).build()).isEqualTo(V);
        assertThat(new IcebergColumnHandle.Builder(pruned).path(ImmutableList.of()).build()).isEqualTo(V);
        assertThat(new IcebergColumnHandle.Builder(pruned).baseColumnIdentity(V.getBaseColumnIdentity()).build()).isEqualTo(V);
        assertThat(new IcebergColumnHandle.Builder(pruned).columnType(BIGINT).build().getVariantPaths()).isEmpty();

        // the six-argument constructor means no path set
        @SuppressWarnings("deprecation")
        IcebergColumnHandle constructed = new IcebergColumnHandle(V.getBaseColumnIdentity(), VARIANT, ImmutableList.of(), VARIANT, true, Optional.empty());
        assertThat(constructed).isEqualTo(V);
        assertThat(constructed.getVariantPaths()).isEmpty();
    }

    @Test
    void testWithVariantPathsRejectsNonVariant()
    {
        IcebergColumnHandle bigint = IcebergColumnHandle.optional(primitiveColumnIdentity(12, "b")).columnType(BIGINT).build();
        assertThatThrownBy(() -> bigint.withVariantPaths(ImmutableSet.of(STATUS)))
                .isInstanceOf(IllegalArgumentException.class);

        ColumnIdentity nestedVariant = new ColumnIdentity(3, "nv", PRIMITIVE, ImmutableList.of());
        ColumnIdentity struct = new ColumnIdentity(5, "r", STRUCT, ImmutableList.of(nestedVariant));
        IcebergColumnHandle nested = IcebergColumnHandle.optional(struct)
                .fieldType(RowType.from(ImmutableList.of(RowType.field("nv", VARIANT))), VARIANT)
                .path(3)
                .build();
        assertThatThrownBy(() -> nested.withVariantPaths(ImmutableSet.of(STATUS)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> V.withVariantPaths(ImmutableSet.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPrunedReadSatisfiesContract()
            throws IOException
    {
        File file = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        // status 2, n 2, country 2, sku 3 (row 0 twice, row 9), qty 3 (rows 0, 1, 9)
        assertThat(typedValueCount(file)).isEqualTo(12);

        List<Variant> whole = readVariants(file, V);
        assertThat(whole).hasSize(ROWS.size());
        for (Set<VariantPath> paths : List.of(Set.of(STATUS), Set.of(COUNTRY), Set.of(SKU))) {
            // read with the id column, so channel alignment is checked too
            List<List<Object>> channels = read(file, ImmutableList.of(ID, V.withVariantPaths(paths)));
            assertThat(channels.get(0)).isEqualTo(ids());
            assertContract(whole, variants(channels.get(1)), paths);
        }
    }

    @Test
    void testPageSourceReadIsPruned()
            throws IOException
    {
        // what the page source returns lacks shredded fields outside the requested paths, so the read was pruned, not whole
        File file = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        assertThat(typedValueCount(file)).isEqualTo(12);
        List<Variant> whole = readVariants(file, V);

        // {status}: the shredded n and the nested user.geo.country are not read
        List<List<Object>> channels = read(file, ImmutableList.of(ID, V.withVariantPaths(Set.of(STATUS))));
        assertThat(channels.get(0)).isEqualTo(ids());
        List<Variant> status = variants(channels.get(1));
        for (int row : List.of(0, 9)) {
            assertThat(extract(whole.get(row), N)).isInstanceOf(Extraction.Value.class);
            assertThat(extract(status.get(row), N)).isEqualTo(new Extraction.Missing());
            assertThat(extract(whole.get(row), COUNTRY)).isInstanceOf(Extraction.Value.class);
            assertThat(extract(status.get(row), COUNTRY)).isEqualTo(new Extraction.Missing());
        }
        assertContract(whole, status, Set.of(STATUS));

        // {user.geo.country}: the shredded top-level status is not read
        List<Variant> country = readVariants(file, V.withVariantPaths(Set.of(COUNTRY)));
        assertThat(extract(whole.get(0), STATUS)).isInstanceOf(Extraction.Value.class);
        assertThat(extract(country.get(0), STATUS)).isEqualTo(new Extraction.Missing());

        // {items[*].sku}: inside each list element, the shredded qty is not read
        VariantPath qty = VariantPath.of(Step.key("items"), Step.element(), Step.key("qty"));
        List<Variant> sku = readVariants(file, V.withVariantPaths(Set.of(SKU)));
        Extraction wholeQty = extract(whole.get(0), qty);
        assertThat(wholeQty).isInstanceOf(Extraction.Elements.class);
        assertThat(((Extraction.Elements) wholeQty).perElement().getFirst()).isInstanceOf(Extraction.Value.class);
        assertThat(extract(sku.get(0), qty)).isEqualTo(new Extraction.Elements(List.of(new Extraction.Missing(), new Extraction.Missing())));
        assertContract(whole, sku, Set.of(SKU));
    }

    @Test
    void testPrunedReadRequestsFewerColumns()
            throws IOException
    {
        File file = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        assertThat(typedValueCount(file)).isEqualTo(12);
        MessageType fileSchema = fileSchema(file);

        assertThat(requestedColumns(fileSchema, ImmutableList.of(V.withVariantPaths(Set.of(STATUS))))).containsExactlyInAnyOrder(
                "v.metadata",
                "v.value",
                "v.typed_value.status.value",
                "v.typed_value.status.typed_value");
        // the id column beside it is requested as before
        assertThat(requestedColumns(fileSchema, ImmutableList.of(ID, V.withVariantPaths(Set.of(STATUS))))).containsExactlyInAnyOrder(
                "id",
                "v.metadata",
                "v.value",
                "v.typed_value.status.value",
                "v.typed_value.status.typed_value");
        // a whole read requests the file's whole group, unchanged
        assertThat(IcebergPageSourceProvider.getRequestedSchema(ImmutableList.of(V), fileSchema))
                .isEqualTo(new MessageType(fileSchema.getName(), fileSchema.getType("v")));
        assertThat(requestedColumns(fileSchema, ImmutableList.of(V))).hasSize(fileSchema.getColumns().size() - 1);
    }

    @Test
    void testWholeAbsorbsPruned()
            throws IOException
    {
        File file = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        assertThat(typedValueCount(file)).isEqualTo(12);
        MessageType fileSchema = fileSchema(file);

        List<IcebergColumnHandle> columns = ImmutableList.of(V.withVariantPaths(Set.of(STATUS)), ID, V);
        // all columns are read
        assertThat(ImmutableSet.copyOf(requestedColumns(fileSchema, columns))).isEqualTo(ImmutableSet.copyOf(requestedColumns(fileSchema, ImmutableList.of(ID, V))));
        assertThat(requestedColumns(fileSchema, columns)).hasSize(fileSchema.getColumns().size());

        List<Variant> whole = readVariants(file, V);
        List<List<Object>> channels = read(file, columns);
        assertThat(channels.get(1)).isEqualTo(ids());
        // both variant channels carry the whole value
        assertContract(whole, variants(channels.get(0)), Set.of(ROOT));
        assertContract(whole, variants(channels.get(2)), Set.of(ROOT));
    }

    @Test
    void testTwoPrunedHandlesUnion()
            throws IOException
    {
        File file = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        assertThat(typedValueCount(file)).isEqualTo(12);
        MessageType fileSchema = fileSchema(file);

        List<IcebergColumnHandle> columns = ImmutableList.of(V.withVariantPaths(Set.of(STATUS)), V.withVariantPaths(Set.of(N)));
        // one read of the union
        assertThat(requestedColumns(fileSchema, columns)).containsExactlyInAnyOrder(
                "v.metadata",
                "v.value",
                "v.typed_value.status.value",
                "v.typed_value.status.typed_value",
                "v.typed_value.n.value",
                "v.typed_value.n.typed_value");
        assertThat(IcebergPageSourceProvider.getRequestedSchema(columns, fileSchema))
                .isEqualTo(IcebergPageSourceProvider.getRequestedSchema(ImmutableList.of(V.withVariantPaths(Set.of(N, STATUS))), fileSchema));

        List<Variant> whole = readVariants(file, V);
        List<List<Object>> channels = read(file, columns);
        assertContract(whole, variants(channels.get(0)), Set.of(STATUS));
        assertContract(whole, variants(channels.get(1)), Set.of(N));
    }

    @Test
    void testUnshreddedFileIgnoresPaths()
            throws IOException
    {
        File file = writeFile("plain.parquet", null, ROWS);
        assertThat(typedValueCount(file)).isEqualTo(0);
        MessageType fileSchema = fileSchema(file);

        assertThat(IcebergPageSourceProvider.getRequestedSchema(ImmutableList.of(V.withVariantPaths(Set.of(STATUS))), fileSchema))
                .isEqualTo(IcebergPageSourceProvider.getRequestedSchema(ImmutableList.of(V), fileSchema));
        List<Variant> whole = readVariants(file, V);
        assertContract(whole, readVariants(file, V.withVariantPaths(Set.of(STATUS, SKU))), Set.of(ROOT));
    }

    @Test
    void testMixedFilesInOneTable()
            throws IOException
    {
        File plain = writeFile("plain.parquet", null, ROWS);
        File statusAndUser = writeFile("status-user.parquet", objectShredding(
                field("status", string()),
                field("user", objectShredding(field("geo", objectShredding(field("country", string())).apply(null, null))).apply(null, null))), ROWS);
        File nAndItems = writeFile("n-items.parquet", objectShredding(
                field("n", int32()),
                field("items", list(org.apache.parquet.schema.Types.requiredGroup()
                        .optional(BINARY).named("value")
                        .addField(objectShredding(field("sku", string()), field("qty", int32())).apply(null, null))
                        .named("element")))), ROWS);
        assertThat(typedValueCount(plain)).isEqualTo(0);
        // status 2, country 2
        assertThat(typedValueCount(statusAndUser)).isEqualTo(4);
        // n 2, sku 3, qty 3
        assertThat(typedValueCount(nAndItems)).isEqualTo(8);

        // one handle, the same for every file of the table
        List<Set<VariantPath>> pathSets = List.of(Set.of(STATUS, N, COUNTRY, SKU), Set.of(STATUS), Set.of(SKU), Set.of(COUNTRY, N));
        List<Variant> reference = readVariants(plain, V);
        for (File file : List.of(plain, statusAndUser, nAndItems)) {
            List<Variant> whole = readVariants(file, V);
            assertContract(reference, whole, Set.of(ROOT));
            for (Set<VariantPath> paths : pathSets) {
                assertContract(whole, readVariants(file, V.withVariantPaths(paths)), paths);
            }
        }
        // a path the file did not shred is read from the value columns along it
        assertThat(requestedColumns(fileSchema(nAndItems), ImmutableList.of(V.withVariantPaths(Set.of(STATUS))))).containsExactlyInAnyOrder(
                "v.metadata",
                "v.value",
                // the witness that tells an object without status from a value that is not an object
                "v.typed_value.n.typed_value");
    }

    @Test
    void testNoPathSetIsUnchanged()
            throws IOException
    {
        File shredded = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        File plain = writeFile("plain.parquet", null, ROWS);
        assertThat(typedValueCount(shredded)).isEqualTo(12);
        MessageType fileSchema = fileSchema(shredded);

        // the requested schema of a scan with no path set is the file's schema for those columns
        assertThat(IcebergPageSourceProvider.getRequestedSchema(ImmutableList.of(ID, V), fileSchema)).isEqualTo(fileSchema);

        List<List<Object>> channels = read(shredded, ImmutableList.of(ID, V));
        assertThat(channels.get(0)).isEqualTo(ids());
        List<Variant> values = variants(channels.get(1));
        assertContract(readVariants(plain, V), values, Set.of(ROOT));
        assertThat(describe(extract(values.get(0), ROOT)))
                .isEqualTo("Value({\"extra\":\"x\",\"items\":[{\"qty\":2,\"sku\":\"a\"},{\"sku\":\"b\"}],\"n\":1,\"status\":\"ok\",\"user\":{\"geo\":{\"city\":\"Paris\",\"country\":\"FR\"}}})");
        assertThat(values.get(4)).isNull();
    }

    @Test
    void testPredicateOnOtherColumnWithPrunedVariant()
            throws IOException
    {
        File file = writeFile("full.parquet", FULL_SHREDDING, ROWS);
        assertThat(typedValueCount(file)).isEqualTo(12);
        IcebergColumnHandle pruned = V.withVariantPaths(Set.of(STATUS));

        // a domain on id prunes the row group through the id column's descriptor, beside the pruned variant group
        TupleDomain<IcebergColumnHandle> noRows = TupleDomain.withColumnDomains(ImmutableMap.of(ID, Domain.singleValue(INTEGER, 100L)));
        assertThat(read(file, ImmutableList.of(ID, pruned), noRows).get(0)).isEmpty();

        // a domain on id that keeps the row group, with IS NULL on the variant: the reader returns the rows, the engine filters them
        TupleDomain<IcebergColumnHandle> someRows = TupleDomain.withColumnDomains(ImmutableMap.of(
                ID, Domain.singleValue(INTEGER, 4L),
                V, Domain.onlyNull(VARIANT)));
        List<List<Object>> channels = read(file, ImmutableList.of(ID, pruned), someRows);
        assertThat(channels.get(0)).isEqualTo(ids());
        assertContract(readVariants(file, V), variants(channels.get(1)), Set.of(STATUS));
    }

    @Test
    void testPrunedReadLayoutErrorNamesTheFile()
            throws IOException
    {
        // Iceberg's writer emits no invalid layout, so this file is written by parquet-java with the table's field ids
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message table {
                  required int32 id = 1;
                  optional group v (VARIANT(1)) = 2 {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group status {
                        optional binary value;
                        optional binary typed_value (STRING);
                      }
                    }
                    optional binary extra;
                  }
                }
                """);
        SimpleGroup row = new SimpleGroup(schema);
        row.append("id", 0);
        row.addGroup("v").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0, 0}));
        File file = directory.resolve("raw.parquet").toFile();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file.toPath()))
                .withType(schema)
                .build()) {
            writer.write(row);
        }
        String location = new LocalInputFile(file).location().toString();
        assertThatThrownBy(() -> read(file, ImmutableList.of(ID, V.withVariantPaths(Set.of(STATUS)))))
                .hasMessage("Unsupported shredded VARIANT layout in column v: unexpected child 'extra' beside value and typed_value (file %s)".formatted(location));
        // the same refusal as the whole read
        assertThatThrownBy(() -> read(file, ImmutableList.of(ID, V)))
                .hasMessage("Unsupported shredded VARIANT layout in column v: unexpected child 'extra' beside value and typed_value (file %s)".formatted(location));
    }

    @Test
    void testSelectiveRebuildSessionProperty()
            throws IOException
    {
        // a filter on id rejects the row with a malformed variant: the session property decides whether that row is still decoded
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message table {
                  required int32 id = 1;
                  optional group v (VARIANT(1)) = 2 {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group status {
                        optional binary value;
                        optional binary typed_value (STRING);
                      }
                    }
                  }
                }
                """);
        int rows = 40;
        int corruptId = 17;
        File corrupt = directory.resolve("corrupt.parquet").toFile();
        File valid = directory.resolve("valid.parquet").toFile();
        for (File file : List.of(corrupt, valid)) {
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file.toPath()))
                    .withType(schema)
                    .build()) {
                for (int id = 0; id < rows; id++) {
                    SimpleGroup row = new SimpleGroup(schema);
                    row.append("id", id);
                    if (id % 9 == 4) {
                        // a SQL NULL variant
                        writer.write(row);
                        continue;
                    }
                    Group v = row.addGroup("v");
                    v.append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0, 0}));
                    if (file == corrupt && id == corruptId) {
                        // an object header, truncated: it cannot be merged with the shredded field
                        v.append("value", Binary.fromConstantByteArray(new byte[] {0x02, 0x05}));
                    }
                    v.addGroup("typed_value").addGroup("status").append("typed_value", "s" + id);
                    writer.write(row);
                }
            }
        }
        // every non-null row's status is in typed_value, so the read goes through the shredded path
        assertThat(typedValueCount(corrupt)).isEqualTo(36);
        assertThat(typedValueCount(valid)).isEqualTo(36);
        String location = new LocalInputFile(corrupt).location().toString();
        assertThat(session(true).getProperty(IcebergSessionProperties.PARQUET_SHREDDED_VARIANT_SELECTIVE_REBUILD, Boolean.class)).isTrue();
        assertThat(session().getProperty(IcebergSessionProperties.PARQUET_SHREDDED_VARIANT_SELECTIVE_REBUILD, Boolean.class)).isTrue();

        List<List<Object>> expected = readRejectingId(valid, corruptId, rows, session(true));
        assertThat(expected.get(0)).hasSize(rows - 1).doesNotContain((long) corruptId);
        assertThat(expected.get(1)).containsNull();
        // on (the default): the rejected row is never decoded, and the surviving rows are as in the valid file
        assertThat(readRejectingId(corrupt, corruptId, rows, session(true))).isEqualTo(expected);
        assertThat(readRejectingId(corrupt, corruptId, rows, session())).isEqualTo(expected);
        // off: the whole batch is decoded before the selection, so the rejected row fails, naming the file
        assertThatThrownBy(() -> readRejectingId(corrupt, corruptId, rows, session(false)))
                .hasMessageStartingWith("Unsupported shredded VARIANT layout in column v: invalid variant encoding in the file: ")
                .hasMessageEndingWith("(file %s)".formatted(location));
        // and with no row rejected, both fail
        assertThatThrownBy(() -> readRejectingId(corrupt, -1, rows, session(true)))
                .hasMessageStartingWith("Unsupported shredded VARIANT layout in column v: invalid variant encoding in the file: ");
    }

    @Test
    void testSelectiveRebuildCatalogProperty()
    {
        IcebergSessionProperties on = new IcebergSessionProperties(
                new IcebergConfig(), new IcebergEncryptionConfig(), new OrcReaderConfig(), new OrcWriterConfig(), new ParquetReaderConfig(), new ParquetWriterConfig());
        IcebergSessionProperties off = new IcebergSessionProperties(
                new IcebergConfig().setParquetShreddedVariantSelectiveRebuild(false), new IcebergEncryptionConfig(), new OrcReaderConfig(), new OrcWriterConfig(), new ParquetReaderConfig(), new ParquetWriterConfig());
        assertThat(isSelectiveRebuild(on)).isTrue();
        assertThat(isSelectiveRebuild(off)).isFalse();
    }

    private static boolean isSelectiveRebuild(IcebergSessionProperties properties)
    {
        return IcebergSessionProperties.isParquetShreddedVariantSelectiveRebuild(
                TestingConnectorSession.builder().setPropertyMetadata(properties.getSessionProperties()).build());
    }

    // Reads id, then selects the rows whose id is not rejectedId, then reads v; the two channels of the surviving rows
    private static List<List<Object>> readRejectingId(File file, int rejectedId, long recordCount, TestingConnectorSession session)
            throws IOException
    {
        LocalInputFile inputFile = new LocalInputFile(file);
        List<List<Object>> channels = ImmutableList.of(new ArrayList<>(), new ArrayList<>());
        try (ConnectorPageSource pageSource = createPageSourceProvider().createPageSource(
                session,
                ImmutableList.of(ID, V),
                SCHEMA,
                PartitionSpec.unpartitioned(),
                new PartitionData(new Object[] {}),
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                TupleDomain.all(),
                TupleDomain.all(),
                inputFile.location().toString(),
                0,
                inputFile.length(),
                inputFile.length(),
                recordCount,
                PARQUET,
                new IcebergTableCredentials(ImmutableMap.of(), ImmutableList.of()),
                OptionalLong.of(0),
                OptionalLong.empty(),
                Optional.empty(),
                newSimpleAggregatedMemoryContext(),
                Optional.empty())) {
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block ids = page.getBlock(0);
                int[] positions = new int[ids.getPositionCount() + 1];
                int size = 0;
                for (int position = 0; position < ids.getPositionCount(); position++) {
                    if (INTEGER.getLong(ids, position) != rejectedId) {
                        // offset 1 into the positions array
                        positions[1 + size++] = position;
                    }
                }
                page.selectPositions(positions, 1, size);
                ids = page.getBlock(0);
                Block variants = page.getBlock(1);
                for (int position = 0; position < size; position++) {
                    channels.get(0).add(INTEGER.getLong(ids, position));
                    channels.get(1).add(variants.isNull(position) ? null : VARIANT.getObject(variants, position));
                }
            }
        }
        return channels;
    }

    // ---- the contract ----

    private static void assertContract(List<Variant> whole, List<Variant> pruned, Set<VariantPath> paths)
    {
        assertThat(pruned).as("rows read for %s", paths).hasSameSizeAs(whole);
        for (VariantPath path : paths) {
            for (int row = 0; row < whole.size(); row++) {
                Extraction expected = extract(whole.get(row), path);
                Extraction actual = extract(pruned.get(row), path);
                if (!same(actual, expected)) {
                    fail("path %s of row %s, reading %s: pruned read gives %s, whole read gives %s".formatted(
                            path, row, paths, describe(actual), describe(expected)));
                }
            }
        }
    }

    private static List<Object> ids()
    {
        List<Object> ids = new ArrayList<>();
        for (int i = 0; i < ROWS.size(); i++) {
            ids.add((long) i);
        }
        return ids;
    }

    @Test
    void testOrcRejectionAndAvroAliases()
            throws IOException
    {
        File parquet = writeFile("reference.parquet", FULL_SHREDDING, ROWS);
        List<IcebergColumnHandle> columns = List.of(
                V.withVariantPaths(Set.of(STATUS)),
                ID,
                V.withVariantPaths(Set.of(COUNTRY)),
                V);
        List<List<Object>> expected = read(parquet, columns);
        for (IcebergFileFormat format : List.of(IcebergFileFormat.ORC, IcebergFileFormat.AVRO)) {
            File file = directory.resolve("aliases." + format.name().toLowerCase(Locale.ENGLISH)).toFile();
            try (FileAppender<Record> writer = format == IcebergFileFormat.ORC
                    ? ORC.write(Files.localOutput(file)).schema(SCHEMA).createWriterFunc(GenericOrcWriter::buildWriter).build()
                    : Avro.write(Files.localOutput(file)).schema(SCHEMA).createWriterFunc(DataWriter::create).build()) {
                for (int i = 0; i < ROWS.size(); i++) {
                    Record row = GenericRecord.create(SCHEMA);
                    row.setField("id", i);
                    row.setField("v", ROWS.get(i) == null ? null : asVariant(ROWS.get(i)));
                    writer.add(row);
                }
            }
            if (format == IcebergFileFormat.ORC) {
                // Iceberg 1.12 writes iceberg.struct-type=VARIANT, while this ORC reader expects
                // iceberg.variant-type=true. Pin the existing whole/pruned rejection equally.
                for (List<IcebergColumnHandle> requested : List.of(List.of(V), columns)) {
                    assertThatThrownBy(() -> read(file, requested, TupleDomain.all(), SCHEMA, format, List.of(), DynamicFilter.EMPTY, Optional.empty()))
                            .isInstanceOf(TrinoException.class)
                            .hasMessageContaining("Cannot read SQL type 'variant'");
                }
                continue;
            }
            List<List<Object>> actual = read(file, columns, TupleDomain.all(), SCHEMA, format, List.of(), DynamicFilter.EMPTY, Optional.empty());
            assertThat(actual.get(1)).isEqualTo(expected.get(1));
            for (int channel : List.of(0, 2, 3)) {
                for (int row = 0; row < ROWS.size(); row++) {
                    assertThat(same(extract((Variant) actual.get(channel).get(row), ROOT), extract((Variant) expected.get(channel).get(row), ROOT)))
                            .as("%s channel %s row %s", format, channel, row).isTrue();
                }
            }
        }
    }

    @Test
    void testRenameAndNameMapping()
            throws IOException
    {
        Schema renamed = new Schema(SCHEMA.findField(1), Types.NestedField.optional(2, "payload", Types.VariantType.get()));
        IcebergColumnHandle payload = IcebergColumnHandle.optional(primitiveColumnIdentity(2, "payload")).columnType(VARIANT).build()
                .withVariantPaths(Set.of(STATUS));
        File withIds = writeFile("renamed.parquet", FULL_SHREDDING, ROWS);
        List<Object> actual = read(withIds, List.of(payload), TupleDomain.all(), renamed, PARQUET, List.of(), DynamicFilter.EMPTY, Optional.empty()).getFirst();
        List<Variant> expected = readVariants(withIds, V);
        for (int row = 0; row < ROWS.size(); row++) {
            assertThat(same(extract((Variant) actual.get(row), STATUS), extract(expected.get(row), STATUS))).isTrue();
        }

        MessageType noIds = MessageTypeParser.parseMessageType(
                """
                message table {
                    required int32 id;
                    optional group v (VARIANT(1)) {
                        required binary metadata;
                        optional binary value;
                        optional group typed_value {
                            required group status { optional binary value; optional binary typed_value (STRING); }
                        }
                    }
                }
                """);
        File mapped = directory.resolve("mapped.parquet").toFile();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(mapped.toPath())).withType(noIds).build()) {
            SimpleGroup row = new SimpleGroup(noIds);
            row.append("id", 0);
            Group value = row.addGroup("v");
            value.append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0, 0}));
            value.addGroup("typed_value").addGroup("status").append("typed_value", "mapped");
            writer.write(row);
        }
        // The mapping retains the old physical name while the current schema uses the new name.
        List<Object> result = read(
                mapped,
                List.of(payload),
                TupleDomain.all(),
                renamed,
                PARQUET,
                List.of(),
                DynamicFilter.EMPTY,
                Optional.of(MappingUtil.create(SCHEMA))).getFirst();
        assertThat(result).hasSize(1);
        assertThat(same(extract((Variant) result.getFirst(), STATUS), new Extraction.Value(Variant.ofString("mapped")))).isTrue();
    }

    @Test
    void testMissingColumnAndInitialDefault()
            throws IOException
    {
        File file = writeFile("missing.parquet", FULL_SHREDDING, ROWS);
        Schema evolved = new Schema(
                SCHEMA.findField(1),
                SCHEMA.findField(2),
                Types.NestedField.optional(3, "missing", Types.VariantType.get()),
                Types.NestedField.required("defaulted").withId(4).ofType(Types.IntegerType.get()).withInitialDefault(Literal.of(42)).build());
        IcebergColumnHandle missing = IcebergColumnHandle.optional(primitiveColumnIdentity(3, "missing")).columnType(VARIANT).build()
                .withVariantPaths(Set.of(STATUS));
        IcebergColumnHandle defaulted = IcebergColumnHandle.required(primitiveColumnIdentity(4, "defaulted")).columnType(INTEGER).build();
        List<List<Object>> result = read(
                file,
                List.of(missing, defaulted, V.withVariantPaths(Set.of(STATUS))),
                TupleDomain.all(),
                evolved,
                PARQUET,
                List.of(),
                DynamicFilter.EMPTY,
                Optional.empty());
        assertThat(result.get(0)).hasSize(ROWS.size()).containsOnlyNulls();
        assertThat(result.get(1)).hasSize(ROWS.size()).containsOnly(42L);
        assertThat(result.get(2)).hasSize(ROWS.size());
    }

    @Test
    void testDynamicFilterAndMetadataColumns()
            throws IOException
    {
        File file = writeFile("dynamic.parquet", FULL_SHREDDING, ROWS);
        IcebergColumnHandle rowPosition = IcebergColumnHandle.required(primitiveColumnIdentity(
                MetadataColumns.ROW_POSITION.fieldId(), "$row_position")).columnType(BIGINT).build();
        List<IcebergColumnHandle> columns = List.of(
                V.withVariantPaths(Set.of(STATUS)),
                IcebergColumnHandle.pathColumnHandle(),
                V.withVariantPaths(Set.of(COUNTRY)),
                rowPosition,
                ID);
        DynamicFilter rejecting = new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return Set.of(ID);
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(INTEGER, 1000L)));
            }
        };
        assertThat(read(file, columns, TupleDomain.all(), SCHEMA, PARQUET, List.of(), rejecting, Optional.empty()))
                .allSatisfy(channel -> assertThat(channel).isEmpty());
        List<List<Object>> result = read(file, columns);
        assertThat(result.get(1)).hasSize(ROWS.size()).containsOnly(Slices.utf8Slice(new LocalInputFile(file).location().toString()));
        assertThat(result.get(3)).isEqualTo(ids());
        List<Variant> whole = readVariants(file, V);
        for (int row = 0; row < ROWS.size(); row++) {
            assertThat(same(extract((Variant) result.get(0).get(row), STATUS), extract(whole.get(row), STATUS))).isTrue();
            assertThat(same(extract((Variant) result.get(2).get(row), COUNTRY), extract(whole.get(row), COUNTRY))).isTrue();
        }
    }

    @Test
    void testEqualityDeleteColumnDemand()
            throws IOException
    {
        File file = writeFile("deleted.parquet", FULL_SHREDDING, ROWS);
        for (int deleteId : List.of(1, 2)) {
            Schema deleteSchema = new Schema(SCHEMA.findField(deleteId));
            File deleteFile = directory.resolve("delete-" + deleteId + ".parquet").toFile();
            try (FileAppender<Record> writer = Parquet.write(Files.localOutput(deleteFile))
                    .schema(deleteSchema).createWriterFunc(GenericParquetWriter::create).build()) {
                Record row = GenericRecord.create(deleteSchema);
                row.set(0, deleteId == 1 ? 0 : asVariant(ROWS.getFirst()));
                writer.add(row);
            }
            LocalInputFile deleteInput = new LocalInputFile(deleteFile);
            DeleteFile delete = new DeleteFile(
                    FileContent.EQUALITY_DELETES,
                    deleteInput.location().toString(),
                    FileFormat.PARQUET,
                    1,
                    deleteInput.length(),
                    List.of(deleteId),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    1,
                    OptionalLong.empty(),
                    Optional.empty(),
                    Optional.empty());
            // The ordinary equality key is absent from outputs. A VARIANT key must demand the whole value,
            // even when a restricted output for that same field ID is already present.
            List<List<Object>> result = read(
                    file,
                    List.of(V.withVariantPaths(Set.of(STATUS)), V.withVariantPaths(Set.of(COUNTRY))),
                    TupleDomain.all(),
                    SCHEMA,
                    PARQUET,
                    List.of(delete),
                    DynamicFilter.EMPTY,
                    Optional.empty());
            assertThat(result.getFirst()).as("delete field %s", deleteId).hasSize(ROWS.size() - 1);
            List<Variant> whole = readVariants(file, V);
            var request = runtimePredicate(V, STATUS, Variant.ofString(utf8Slice("ok")));
            List<List<Object>> runtime = read(
                    file,
                    List.of(request, V.withVariantPaths(Set.of(COUNTRY)), request),
                    TupleDomain.all(),
                    SCHEMA,
                    PARQUET,
                    List.of(delete),
                    DynamicFilter.EMPTY,
                    Optional.empty());
            assertThat(runtime.getFirst()).hasSize(ROWS.size() - 1);
            for (int row = 1; row < ROWS.size(); row++) {
                long decision = request.evaluator().evaluateReference(whole.get(row)).code();
                assertThat(runtime.get(0).get(row - 1)).isEqualTo(decision);
                assertThat(runtime.get(2).get(row - 1)).isEqualTo(decision);
            }

            for (int row = 1; row < ROWS.size(); row++) {
                assertThat(same(extract((Variant) result.get(0).get(row - 1), STATUS), extract(whole.get(row), STATUS))).isTrue();
                assertThat(same(extract((Variant) result.get(1).get(row - 1), COUNTRY), extract(whole.get(row), COUNTRY))).isTrue();
            }
        }
    }

    @Test
    void testRuntimeVariantPredicatesAndFormatFallbacks()
            throws IOException
    {
        IcebergVariantPredicateColumn request = runtimePredicate(V, STATUS, Variant.ofString(utf8Slice("ok")));
        for (boolean shredded : List.of(false, true)) {
            File file = writeFile("runtime-" + shredded + ".parquet", shredded ? FULL_SHREDDING : null, ROWS);
            List<Variant> expected = readVariants(file, V);
            List<List<Object>> result = read(file, List.of(request, ID, V.withVariantPaths(Set.of(COUNTRY)), request, V));
            List<List<Object>> predicateOnly = read(file, List.of(request));
            for (int row = 0; row < ROWS.size(); row++) {
                long decision = request.evaluator().evaluateReference(expected.get(row)).code();
                assertThat(result.get(0).get(row)).isEqualTo(decision);
                assertThat(result.get(3).get(row)).isEqualTo(decision);
                assertThat(predicateOnly.getFirst().get(row)).isEqualTo(decision);
                assertThat(result.get(1).get(row)).isEqualTo((long) row);
                assertThat(same(extract((Variant) result.get(4).get(row), ROOT), extract(expected.get(row), ROOT))).isTrue();
                assertThat(same(extract((Variant) result.get(2).get(row), COUNTRY), extract(expected.get(row), COUNTRY))).isTrue();
            }
        }
        File avro = directory.resolve("runtime.avro").toFile();
        try (FileAppender<Record> writer = Avro.write(Files.localOutput(avro)).schema(SCHEMA)
                .createWriterFunc(DataWriter::create).build()) {
            for (int i = 0; i < ROWS.size(); i++) {
                Record record = GenericRecord.create(SCHEMA);
                record.set(0, i);
                record.set(1, ROWS.get(i) == null ? null : asVariant(ROWS.get(i)));
                writer.add(record);
            }
        }
        List<List<Object>> result = read(
                avro,
                List.of(request, ID, V, request),
                TupleDomain.all(),
                SCHEMA,
                IcebergFileFormat.AVRO,
                List.of(),
                DynamicFilter.EMPTY,
                Optional.empty());
        for (int row = 0; row < ROWS.size(); row++) {
            long decision = request.evaluator().evaluateReference((Variant) result.get(2).get(row)).code();
            assertThat(result.get(0).get(row)).isEqualTo(decision);
            assertThat(result.get(3).get(row)).isEqualTo(decision);
        }
    }

    @Test
    void testMissingRuntimeVariantColumn()
            throws IOException
    {
        File file = writeFile("runtime-missing.parquet", FULL_SHREDDING, ROWS);
        Schema schema = new Schema(SCHEMA.findField(1), SCHEMA.findField(2), Types.NestedField.optional(3, "absent", Types.VariantType.get()));
        IcebergColumnHandle missing = IcebergColumnHandle.optional(primitiveColumnIdentity(3, "absent")).columnType(VARIANT).build();
        var request = runtimePredicate(missing, STATUS, Variant.ofString(utf8Slice("ok")));
        assertThat(read(file, List.of(request), TupleDomain.all(), schema, PARQUET, List.of(), DynamicFilter.EMPTY, Optional.empty()).getFirst())
                .containsExactlyElementsOf(Collections.nCopies(ROWS.size(), 0L));
    }

    private static IcebergVariantPredicateColumn runtimePredicate(IcebergColumnHandle source, VariantPath path, Variant expected)
    {
        return new IcebergVariantPredicateColumn(source.withVariantPaths(Set.of(path)), new VariantPredicateEvaluator()
        {
            @Override
            public Decision evaluate(VariantBatchAccess batch, int position)
            {
                VariantPathProbe probe = batch.probe(path, position).getFirst();
                return switch (probe.state()) {
                    case TYPED_VALUE -> probe.value().equals(expected) ? Decision.TRUE : Decision.FALSE;
                    case UNRESOLVED -> evaluateReference(batch.reference(position).orElse(null));
                    case SQL_NULL, MISSING, WRONG_SHAPE -> Decision.FALSE;
                };
            }

            @Override
            public Decision evaluateReference(Variant document)
            {
                Extraction value = extract(document, path);
                return value instanceof Extraction.Value actual && actual.value().equals(expected) ? Decision.TRUE : Decision.FALSE;
            }
        });
    }

    // ---- reading through the page source provider ----

    private static JsonCodec<IcebergColumnHandle> codec()
    {
        JsonMapper jsonMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get();
        return new JsonCodecFactory(jsonMapper).jsonCodec(IcebergColumnHandle.class);
    }

    private static List<String> requestedColumns(MessageType fileSchema, List<IcebergColumnHandle> columns)
    {
        return IcebergPageSourceProvider.getRequestedSchema(columns, fileSchema).getColumns().stream()
                .map(column -> String.join(".", column.getPath()))
                .collect(toImmutableList());
    }

    private static MessageType fileSchema(File file)
            throws IOException
    {
        ParquetMetadata metadata = getParquetFileMetadata(new LocalInputFile(file));
        return metadata.getFileMetaData().getSchema();
    }

    private static List<Variant> readVariants(File file, IcebergColumnHandle column)
            throws IOException
    {
        return variants(read(file, ImmutableList.of(column)).get(0));
    }

    private static List<Variant> variants(List<Object> channel)
    {
        return channel.stream().map(Variant.class::cast).collect(Collectors.toList());
    }

    private static List<List<Object>> read(File file, List<? extends ColumnHandle> columns)
            throws IOException
    {
        return read(file, columns, TupleDomain.all());
    }

    private static List<List<Object>> read(File file, List<? extends ColumnHandle> columns, TupleDomain<IcebergColumnHandle> predicate)
            throws IOException
    {
        return read(file, columns, predicate, SCHEMA, PARQUET, List.of(), DynamicFilter.EMPTY, Optional.empty());
    }

    private static List<List<Object>> read(
            File file,
            List<? extends ColumnHandle> columns,
            TupleDomain<IcebergColumnHandle> predicate,
            Schema schema,
            IcebergFileFormat format,
            List<DeleteFile> deletes,
            DynamicFilter dynamicFilter,
            Optional<NameMapping> nameMapping)
            throws IOException
    {
        LocalInputFile inputFile = new LocalInputFile(file);
        List<List<Object>> channels = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            channels.add(new ArrayList<>());
        }
        try (ConnectorPageSource pageSource = createPageSourceProvider().createPageSource(
                session(),
                columns,
                schema,
                PartitionSpec.unpartitioned(),
                new PartitionData(new Object[] {}),
                deletes,
                dynamicFilter,
                predicate,
                TupleDomain.all(),
                inputFile.location().toString(),
                0,
                inputFile.length(),
                inputFile.length(),
                ROWS.size(),
                format,
                new IcebergTableCredentials(ImmutableMap.of(), ImmutableList.of()),
                OptionalLong.of(0),
                OptionalLong.empty(),
                nameMapping,
                newSimpleAggregatedMemoryContext(),
                Optional.empty())) {
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                for (int channel = 0; channel < columns.size(); channel++) {
                    Block block = page.getBlock(channel);
                    Type type = columns.get(channel) instanceof IcebergVariantPredicateColumn ? TINYINT : ((IcebergColumnHandle) columns.get(channel)).getType();
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        channels.get(channel).add(readNativeValue(type, block, position));
                    }
                }
            }
        }
        return channels;
    }

    private static TestingConnectorSession session()
    {
        return sessionBuilder().build();
    }

    private static TestingConnectorSession session(boolean selectiveRebuild)
    {
        return sessionBuilder()
                .setPropertyValues(ImmutableMap.of(IcebergSessionProperties.PARQUET_SHREDDED_VARIANT_SELECTIVE_REBUILD, selectiveRebuild))
                .build();
    }

    private static TestingConnectorSession.Builder sessionBuilder()
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new IcebergSessionProperties(
                        new IcebergConfig(),
                        new IcebergEncryptionConfig(),
                        new OrcReaderConfig(),
                        new OrcWriterConfig(),
                        new ParquetReaderConfig(),
                        new ParquetWriterConfig()).getSessionProperties());
    }

    private static IcebergPageSourceProvider createPageSourceProvider()
    {
        BlocksHashFactory blocksHashFactory = new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators())).createBlocksHashFactory();
        return new IcebergPageSourceProvider(
                new DefaultIcebergFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)),
                FILE_IO_FACTORY,
                new FileFormatDataSourceStats(),
                new OrcReaderConfig().toOrcReaderOptions(),
                new ParquetReaderConfig().toParquetReaderOptions(),
                TESTING_TYPE_MANAGER,
                ParquetFooterCache.noop(),
                blocksHashFactory,
                ENCRYPTION_MANAGER_FACTORY,
                _ -> {},
                new IcebergConfig().getDomainCompactionThreshold());
    }

    // ---- writing with Iceberg's writer ----

    private File writeFile(String name, VariantShreddingFunction shredding, List<Object> variants)
            throws IOException
    {
        File file = directory.resolve(name).toFile();
        Parquet.WriteBuilder builder = Parquet.write(Files.localOutput(file))
                .schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create);
        if (shredding != null) {
            builder.variantShreddingFunc(shredding);
        }
        try (FileAppender<Record> appender = builder.build()) {
            for (int i = 0; i < variants.size(); i++) {
                Record record = GenericRecord.create(SCHEMA);
                record.setField("id", i);
                record.setField("v", variants.get(i) == null ? null : asVariant(variants.get(i)));
                appender.add(record);
            }
        }
        return file;
    }

    /**
     * Non-null values in the file's {@code typed_value} leaf columns.
     */
    private static long typedValueCount(File file)
            throws IOException
    {
        long count = 0;
        for (BlockMetadata block : getParquetFileMetadata(new LocalInputFile(file)).getBlocks()) {
            for (ColumnChunkMetadata chunk : block.columns()) {
                List<String> path = List.of(chunk.getPath().toArray());
                if (!path.getLast().equals("typed_value")) {
                    continue;
                }
                Statistics<?> statistics = chunk.getStatistics();
                assertThat(statistics != null && statistics.isNumNullsSet()).as("statistics of " + path).isTrue();
                count += chunk.getValueCount() - statistics.getNumNulls();
            }
        }
        return count;
    }

    private static org.apache.iceberg.variants.Variant asVariant(Object value)
    {
        if (value instanceof VariantValue variantValue) {
            return org.apache.iceberg.variants.Variant.of(Variants.emptyMetadata(), variantValue);
        }
        TreeSet<String> names = new TreeSet<>();
        collectFieldNames(value, names);
        VariantMetadata metadata = names.isEmpty() ? Variants.emptyMetadata() : Variants.metadata(names);
        return org.apache.iceberg.variants.Variant.of(metadata, toValue(value, metadata));
    }

    private static void collectFieldNames(Object value, Set<String> names)
    {
        if (value instanceof ObjectSpec spec) {
            names.addAll(spec.fields().keySet());
            spec.fields().values().forEach(fieldValue -> collectFieldNames(fieldValue, names));
        }
        else if (value instanceof List<?> list) {
            list.forEach(element -> collectFieldNames(element, names));
        }
    }

    private static VariantValue toValue(Object value, VariantMetadata metadata)
    {
        if (value instanceof ObjectSpec spec) {
            ShreddedObject object = Variants.object(metadata);
            spec.fields().forEach((name, fieldValue) -> object.put(name, toValue(fieldValue, metadata)));
            return object;
        }
        if (value instanceof List<?> list) {
            ValueArray array = Variants.array();
            list.forEach(element -> array.add(toValue(element, metadata)));
            return array;
        }
        return switch (value) {
            case VariantValue variantValue -> variantValue;
            case String string -> Variants.of(string);
            case Integer integer -> Variants.of(integer);
            default -> throw new IllegalArgumentException("Unsupported test value: " + value);
        };
    }

    private record ObjectSpec(Map<String, Object> fields) {}

    /**
     * An object variant: alternating field names and values.
     */
    private static ObjectSpec obj(Object... nameValuePairs)
    {
        ImmutableMap.Builder<String, Object> fields = ImmutableMap.builder();
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            fields.put((String) nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return new ObjectSpec(fields.buildOrThrow());
    }

    // ---- shredding functions ----

    private record ShreddedField(String name, org.apache.parquet.schema.Type typedValue) {}

    private static ShreddedField field(String name, org.apache.parquet.schema.Type typedValue)
    {
        return new ShreddedField(name, typedValue);
    }

    private static VariantShreddingFunction objectShredding(ShreddedField... fields)
    {
        return (_, _) -> {
            GroupBuilder<GroupType> object = org.apache.parquet.schema.Types.optionalGroup();
            for (ShreddedField field : fields) {
                object.addField(org.apache.parquet.schema.Types.requiredGroup()
                        .optional(BINARY).named("value")
                        .addField(field.typedValue())
                        .named(field.name()));
            }
            return object.named("typed_value");
        };
    }

    private static org.apache.parquet.schema.Type list(org.apache.parquet.schema.Type element)
    {
        return org.apache.parquet.schema.Types.optionalList().element(element).named("typed_value");
    }

    private static org.apache.parquet.schema.Type primitive(PrimitiveTypeName type, LogicalTypeAnnotation annotation)
    {
        return org.apache.parquet.schema.Types.optional(type).as(annotation).named("typed_value");
    }

    private static org.apache.parquet.schema.Type string()
    {
        return primitive(BINARY, stringType());
    }

    private static org.apache.parquet.schema.Type int32()
    {
        return primitive(INT32, intType(32, true));
    }
}
