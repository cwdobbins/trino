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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.VariantShreddingFunction;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiConsumer;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reads Parquet VARIANT columns stored in the shredded layout through the Iceberg connector.
 * The data files are written by Iceberg's own Parquet writer, and every file that is meant to be
 * shredded is checked from its footer to hold values in {@code typed_value} columns.
 */
final class TestIcebergShreddedVariant
        extends AbstractTestQueryFramework
{
    private static final Schema ID_AND_VARIANT = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "v", Types.VariantType.get()));

    private HiveMetastore metastore;
    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().mkdirs();

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "iceberg.format-version", "3",
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true"));

        metastore = getHiveMetastore(queryRunner);
        catalog = getTrinoCatalog(metastore, getFileSystemFactory(queryRunner), "iceberg");

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of(
                "hive.security", "allow-all",
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA tpch");
        return queryRunner;
    }

    @Test
    void testReadShreddedFile()
            throws IOException
    {
        List<Record> rows = ImmutableList.of(
                row(0, obj("name", "Alice", "age", 30, "city", "Paris")),
                row(1, obj("name", "Bob", "age", 41)),
                row(2, obj("name", "Carol")),
                // age does not match the shredded type, so it stays in the residual value
                row(3, obj("name", "Dave", "age", "forty")),
                row(4, null),
                row(5, Variants.of("a plain string")),
                row(6, Variants.ofNull()),
                row(7, obj()),
                row(8, obj("other", true)));

        String shredded = createTable("test_shredded_file");
        long typedValues = appendVariantFile(shredded, objectShredding(field("name", string()), field("age", int32())), rows);
        // name in rows 0..3 and age in rows 0 and 1: 4 + 2
        assertThat(typedValues).isEqualTo(6);

        String plain = createTable("test_unshredded_file");
        assertThat(appendVariantFile(plain, null, rows)).isEqualTo(0);

        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + shredded))
                .matches(
                        """
                        VALUES
                            (0, JSON '{"age":30,"city":"Paris","name":"Alice"}'),
                            (1, JSON '{"age":41,"name":"Bob"}'),
                            (2, JSON '{"name":"Carol"}'),
                            (3, JSON '{"age":"forty","name":"Dave"}'),
                            (4, CAST(NULL AS JSON)),
                            (5, JSON '"a plain string"'),
                            (6, JSON 'null'),
                            (7, JSON '{}'),
                            (8, JSON '{"other":true}')
                        """);
        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + shredded))
                .matches("SELECT id, CAST(v AS JSON) FROM " + plain);
        assertThat(query("SELECT count(*), count(v) FROM " + shredded)).matches("VALUES (BIGINT '9', BIGINT '8')");
        // Iceberg's writer records bounds for a shredded variant column; the split source must not read them as a primitive
        assertThat(query("SELECT count(*) FROM " + shredded + " WHERE v IS NULL")).matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM " + shredded + " WHERE v IS NOT NULL")).matches("VALUES BIGINT '8'");
    }

    @Test
    void testMixedLayoutsInOneTable()
            throws IOException
    {
        String table = createTable("test_mixed_layouts");
        long plain = appendVariantFile(table, null, ImmutableList.of(
                row(0, obj("name", "Alice", "age", 30)),
                row(1, obj("name", "Bob"))));
        // one file shreds name, another shreds age (and a mixed-case field), both written by the same writer
        long byName = appendVariantFile(table, objectShredding(field("name", string())), ImmutableList.of(
                row(2, obj("name", "Carol", "age", 52)),
                row(3, obj("name", "Dave"))));
        long byAge = appendVariantFile(table, objectShredding(field("age", int32()), field("eventType", string())), ImmutableList.of(
                row(4, obj("name", "Erin", "age", 61, "eventType", "login")),
                row(5, obj("age", 70))));
        assertThat(plain).isEqualTo(0);
        assertThat(byName).isEqualTo(2);
        assertThat(byAge).isEqualTo(3);

        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + table))
                .matches(
                        """
                        VALUES
                            (0, JSON '{"age":30,"name":"Alice"}'),
                            (1, JSON '{"name":"Bob"}'),
                            (2, JSON '{"age":52,"name":"Carol"}'),
                            (3, JSON '{"name":"Dave"}'),
                            (4, JSON '{"age":61,"eventType":"login","name":"Erin"}'),
                            (5, JSON '{"age":70}')
                        """);
    }

    @Test
    void testSubscriptOnShreddedField()
            throws IOException
    {
        String table = createTable("test_subscript");
        assertThat(appendVariantFile(table, objectShredding(field("name", string()), field("age", int32())), ImmutableList.of(
                row(0, obj("name", "Alice", "age", 30)),
                row(1, obj("name", "Bob", "age", 41)),
                row(2, obj("name", "Carol")),
                row(3, obj("name", "Dave", "age", "forty")),
                row(4, null)))).isEqualTo(6);

        assertThat(query("SELECT id, CAST(v['name'] AS varchar), CAST(v['age'] AS integer) FROM " + table + " WHERE id < 3"))
                .matches("VALUES (0, VARCHAR 'Alice', 30), (1, VARCHAR 'Bob', 41), (2, VARCHAR 'Carol', CAST(NULL AS integer))");
        assertThat(query("SELECT id, CAST(v['age'] AS JSON) FROM " + table + " WHERE id IN (3, 4)"))
                .matches("VALUES (3, JSON '\"forty\"'), (4, CAST(NULL AS JSON))");
        assertThat(query("SELECT sum(CAST(v['age'] AS integer)) FROM " + table + " WHERE id < 3"))
                .matches("VALUES BIGINT '71'");
    }

    @Test
    void testFilterOnOtherColumnSkipsRows()
            throws IOException
    {
        String table = createTable("test_filter_other_column");
        List<Record> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(row(i, i % 10 == 7 ? null : obj("name", "name-" + i, "age", i, "pad", "x".repeat(40))));
        }
        long typedValues = appendVariantFile(table, objectShredding(field("name", string()), field("age", int32())), rows);
        assertThat(typedValues).isEqualTo(1800);
        assertThat(rowGroupCount(table)).isGreaterThan(3);
        // rows are written in id order, so a row group's first id is the number of rows before it
        assertThat(rowGroupStartIds(table)).contains(450L);

        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + table + " WHERE id = 523"))
                .matches("VALUES (523, JSON '{\"age\":523,\"name\":\"name-523\",\"pad\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}')");
        // the row with a SQL NULL variant
        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + table + " WHERE id = 517"))
                .matches("VALUES (517, CAST(NULL AS JSON))");
        // straddles the row group boundary at id 450 (checked above) and contains NULL variants (ids 427, 437, 447, 457, 467)
        assertThat(query("SELECT count(*), count(v), sum(CAST(v['age'] AS integer)) FROM " + table + " WHERE id BETWEEN 425 AND 474"))
                .matches("VALUES (BIGINT '50', BIGINT '45', BIGINT '20240')");
        assertThat(query("SELECT count(*), count(v), sum(CAST(v['age'] AS integer)) FROM " + table + " WHERE id BETWEEN 100 AND 899"))
                .matches("VALUES (BIGINT '800', BIGINT '720', BIGINT '359440')");
        assertThat(query("SELECT count(*) FROM " + table + " WHERE id >= 5000")).matches("VALUES BIGINT '0'");
    }

    @Test
    void testVariantNotSelected()
            throws IOException
    {
        String table = createTable("test_variant_not_selected");
        appendVariantFile(table, objectShredding(field("name", string())), ImmutableList.of(
                row(0, obj("name", "Alice")),
                row(1, null),
                row(2, obj("name", "Carol", "age", 3))));

        assertThat(query("SELECT id FROM " + table)).matches("VALUES 0, 1, 2");
        assertThat(query("SELECT count(*) FROM " + table)).matches("VALUES BIGINT '3'");
        assertThat(query("SELECT count(*) FROM " + table + " WHERE v IS NOT NULL")).matches("VALUES BIGINT '2'");
    }

    @Test
    void testNestedVariantColumn()
            throws IOException
    {
        Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(3, "v", Types.VariantType.get()),
                Types.NestedField.optional(4, "n", Types.IntegerType.get()));
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "r", struct));
        String table = createTable("test_nested_variant", schema);

        List<Record> rows = new ArrayList<>();
        rows.add(nestedRow(schema, struct, 0, obj("name", "Alice", "age", 30), 10));
        rows.add(nestedRow(schema, struct, 1, null, 11));
        rows.add(nestedRow(schema, struct, 2, obj("name", "Carol"), null));
        Record nullRow = GenericRecord.create(schema);
        nullRow.setField("id", 3);
        rows.add(nullRow);

        long typedValues = appendFile(table, objectShredding(field("name", string()), field("age", int32())), rows);
        assertThat(typedValues).isEqualTo(3);

        assertThat(query("SELECT id, CAST(r.v AS JSON), r.n FROM " + table))
                .matches(
                        """
                        VALUES
                            (0, JSON '{"age":30,"name":"Alice"}', 10),
                            (1, CAST(NULL AS JSON), 11),
                            (2, JSON '{"name":"Carol"}', CAST(NULL AS integer)),
                            (3, CAST(NULL AS JSON), CAST(NULL AS integer))
                        """);
        assertThat(query("SELECT id, r IS NULL FROM " + table + " WHERE id = 3")).matches("VALUES (3, true)");
        assertThat(query("SELECT id, CAST(r.v['name'] AS varchar) FROM " + table + " WHERE id < 3"))
                .matches("VALUES (0, VARCHAR 'Alice'), (1, CAST(NULL AS varchar)), (2, VARCHAR 'Carol')");
    }

    @Test
    void testShreddedVariantInArrayAndMapColumns()
            throws IOException
    {
        // VARIANT as an array element and as a map value, read through the Iceberg converter's array and map branches
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.ListType.ofOptional(3, Types.VariantType.get())),
                Types.NestedField.optional(4, "m", Types.MapType.ofOptional(5, 6, Types.StringType.get(), Types.VariantType.get())));
        Variant variantNull = Variant.of(Variants.emptyMetadata(), Variants.ofNull());
        List<Record> rows = new ArrayList<>();
        rows.add(collectionRow(
                schema,
                0,
                Arrays.asList(obj("name", "Alice", "age", 30), "scalar", null, variantNull),
                mapOf("x", obj("name", "Bob"), "y", null, "z", variantNull)));
        rows.add(collectionRow(schema, 1, ImmutableList.of(), ImmutableMap.of()));
        rows.add(collectionRow(schema, 2, null, null));
        rows.add(collectionRow(schema, 3, ImmutableList.of(obj("name", "Carol", "age", "old")), mapOf("w", obj("age", 7, "extra", true))));
        rows.add(collectionRow(schema, 4, ImmutableList.of(obj()), mapOf("e", obj())));
        VariantShreddingFunction shredding = objectShredding(field("name", string()), field("age", int32()));

        String table = createTable("test_variant_in_array_and_map", schema);
        // Alice's name and age, Bob's name, Carol's name (her age is a string) and the 7
        assertThat(appendFile(table, shredding, rows)).isEqualTo(5);
        String plain = createTable("test_variant_in_array_and_map_plain", schema);
        assertThat(appendFile(plain, null, rows)).isEqualTo(0);

        String arrayAndMap = "SELECT id, transform(a, x -> CAST(x AS JSON)), transform(a, x -> x IS NULL), transform_values(m, (k, x) -> CAST(x AS JSON)), transform_values(m, (k, x) -> x IS NULL) FROM ";
        assertThat(query(arrayAndMap + table)).matches(arrayAndMap + plain);
        // a SQL NULL element or map value stays SQL NULL; a variant null does not
        assertThat(query("SELECT id, transform(a, x -> x IS NULL), transform_values(m, (k, x) -> x IS NULL) FROM " + table + " WHERE id = 0"))
                .matches("VALUES (0, ARRAY[false, false, true, false], MAP(ARRAY[VARCHAR 'x', VARCHAR 'y', VARCHAR 'z'], ARRAY[false, true, false]))");
        assertThat(query("SELECT id, CAST(a[1] AS JSON), CAST(a[2] AS JSON), CAST(m['x'] AS JSON) FROM " + table + " WHERE id = 0"))
                .matches("VALUES (0, JSON '{\"age\":30,\"name\":\"Alice\"}', JSON '\"scalar\"', JSON '{\"name\":\"Bob\"}')");
        assertThat(query("SELECT id, cardinality(a), cardinality(m), a IS NULL, m IS NULL FROM " + table))
                .matches("VALUES (0, BIGINT '4', BIGINT '3', false, false), (1, 0, 0, false, false), (2, NULL, NULL, true, true), (3, 1, 1, false, false), (4, 1, 1, false, false)");
        assertThat(query("SELECT id, CAST(a[1] AS JSON), CAST(element_at(m, 'w') AS JSON), CAST(element_at(m, 'e') AS JSON) FROM " + table + " WHERE id IN (3, 4)"))
                .matches("VALUES (3, JSON '{\"age\":\"old\",\"name\":\"Carol\"}', JSON '{\"age\":7,\"extra\":true}', CAST(NULL AS JSON)), (4, JSON '{}', CAST(NULL AS JSON), JSON '{}')");
    }

    @Test
    void testShreddedListInsideVariant()
            throws IOException
    {
        // Iceberg's writer shreds a list inside the variant: tags as a list of strings, items as a list of small objects
        VariantShreddingFunction shredding = objectShredding(
                field("tags", list(org.apache.parquet.schema.Types.requiredGroup()
                        .optional(BINARY).named("value")
                        .addField(string())
                        .named("element"))),
                field("items", list(org.apache.parquet.schema.Types.requiredGroup()
                        .optional(BINARY).named("value")
                        .addField(objectShredding(field("sku", string()), field("qty", int32())).apply(null, null))
                        .named("element"))));
        Variant variantNull = Variant.of(Variants.emptyMetadata(), Variants.ofNull());
        List<Record> rows = ImmutableList.of(
                row(0, obj("tags", ImmutableList.of("a", "b", "c"), "items", ImmutableList.of(obj("sku", "s1", "qty", 1), obj("sku", "s2", "qty", 2)))),
                row(1, obj("tags", ImmutableList.of(), "items", ImmutableList.of())),
                row(2, obj("tags", "not a list", "items", ImmutableList.of(obj("sku", "s3"), 5))),
                row(3, obj("tags", ImmutableList.of("x", 1, Variants.ofNull()), "items", Variants.ofNull())),
                row(4, null),
                row(5, "scalar"),
                row(6, obj()),
                row(7, variantNull));

        String table = createTable("test_shredded_list");
        long typedValues = appendVariantFile(table, shredding, rows);
        // tags a, b, c, x and the items' s1, 1, s2, 2 and s3: every typed value sits in an element of a shredded list
        assertThat(typedValues).isEqualTo(9);
        assertThat(typedValuesInLists(table)).isEqualTo(9);

        String plain = createTable("test_shredded_list_plain");
        assertThat(appendVariantFile(plain, null, rows)).isEqualTo(0);

        assertThat(query("SELECT id, CAST(v AS JSON), v IS NULL FROM " + table))
                .matches("SELECT id, CAST(v AS JSON), v IS NULL FROM " + plain);
        assertThat(query("SELECT CAST(v AS JSON) FROM " + table + " WHERE id IN (0, 2)"))
                .matches(
                        """
                        VALUES
                            JSON '{"items":[{"qty":1,"sku":"s1"},{"qty":2,"sku":"s2"}],"tags":["a","b","c"]}',
                            JSON '{"items":[{"sku":"s3"},5],"tags":"not a list"}'
                        """);
        assertThat(query("SELECT CAST(v['tags'][2] AS varchar), CAST(v['items'][2]['qty'] AS integer) FROM " + table + " WHERE id = 0"))
                .matches("VALUES (VARCHAR 'b', 2)");
    }

    @Test
    void testTimestampAndUuidFields()
            throws IOException
    {
        String table = createTable("test_timestamp_uuid");
        String uuid = "20050910-1330-11e9-ffff-2a86e4085a59";
        List<Record> rows = ImmutableList.of(
                row(0, obj(
                        "ts",
                        Variants.ofIsoTimestampntz("2024-01-15T12:30:45.123456"),
                        "tstz",
                        Variants.ofIsoTimestamptz("2024-01-15T12:30:45.123456+00:00"),
                        "u",
                        Variants.ofUUID(uuid),
                        "d",
                        Variants.ofIsoDate("2024-01-15"),
                        "dbl",
                        1.5d,
                        "flag",
                        true,
                        "big",
                        1234567890123L)),
                row(1, obj("ts", "not a timestamp", "u", "not a uuid", "d", 7)));

        long typedValues = appendVariantFile(table, objectShredding(
                field("ts", timestamp(false)),
                field("tstz", timestamp(true)),
                field("u", uuid()),
                field("d", date()),
                field("dbl", doubleType()),
                field("flag", bool()),
                field("big", int64())), rows);

        String plain = createTable("test_timestamp_uuid_plain");
        assertThat(appendVariantFile(plain, null, rows)).isEqualTo(0);

        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + table))
                .matches("SELECT id, CAST(v AS JSON) FROM " + plain);
        assertThat(query("SELECT CAST(v AS JSON) FROM " + table + " WHERE id = 1"))
                .matches("VALUES JSON '{\"d\":7,\"ts\":\"not a timestamp\",\"u\":\"not a uuid\"}'");
        // The literal is how Trino renders each of these variant types as JSON (found by casting a ROW of the same
        // values to VARIANT and then JSON, with no file involved)
        assertThat(query("SELECT CAST(v AS JSON) FROM " + table + " WHERE id = 0"))
                .matches("VALUES JSON '{\"big\":1234567890123,\"d\":\"2024-01-15\",\"dbl\":1.5,\"flag\":true,\"ts\":\"2024-01-15 12:30:45.123456\",\"tstz\":\"2024-01-15 12:30:45.123456 UTC\",\"u\":\"20050910-1330-11e9-ffff-2a86e4085a59\"}'");
        assertThat(query("SELECT CAST(v AS JSON) FROM " + table + " WHERE id = 0"))
                .matches("SELECT CAST(v AS JSON) FROM " + plain + " WHERE id = 0");
        // the seven fields of row 0 are shredded; row 1's values do not match their columns' types
        assertThat(typedValues).isEqualTo(7);
    }

    @Test
    void testMetadataAndStatisticsOnShreddedFile()
            throws IOException
    {
        String table = createTable("test_shredded_metadata");
        assertThat(appendVariantFile(table, objectShredding(field("name", string())), ImmutableList.of(
                row(0, obj("name", "Alice")),
                row(1, null),
                row(2, obj("name", "Carol"))))).isEqualTo(2);

        // Iceberg's writer records file bounds for the variant column; none of the metadata paths may choke on them
        assertThat(query("SELECT record_count FROM \"" + table + "$files\"")).matches("VALUES BIGINT '3'");
        assertThat(query("SELECT lower_bounds IS NOT NULL FROM \"" + table + "$files\"")).matches("VALUES true");
        assertThat(query("SHOW STATS FOR " + table)).skippingTypesCheck().containsAll("VALUES ('id', NULL, NULL, 0e0, NULL, '0', '2'), ('v', NULL, NULL, 0.3333333333333333e0, NULL, NULL, NULL)");
        assertThat(query("SELECT a.id, CAST(b.v AS JSON) FROM " + table + " a JOIN " + table + " b ON a.id = b.id WHERE a.id = 2"))
                .matches("VALUES (2, JSON '{\"name\":\"Carol\"}')");
    }

    @Test
    void testTypeMismatchStaysInResidualValue()
            throws IOException
    {
        String table = createTable("test_type_mismatch");
        List<Record> rows = ImmutableList.of(
                row(0, obj("n", 5L)),
                row(1, obj("n", 6)),
                row(2, obj("n", 7.5d)),
                row(3, obj("n", "8")));
        long typedValues = appendVariantFile(table, objectShredding(field("n", int64())), rows);
        // Iceberg's writer shreds a value only into a column of exactly its type: the INT32, the double and the string stay in value
        assertThat(typedValues).isEqualTo(1);

        assertThat(query("SELECT id, CAST(v AS JSON) FROM " + table))
                .matches(
                        """
                        VALUES
                            (0, JSON '{"n":5}'),
                            (1, JSON '{"n":6}'),
                            (2, JSON '{"n":7.5}'),
                            (3, JSON '{"n":"8"}')
                        """);
    }

    @Test
    void testEntriesAndPartitionsOnShreddedFile()
            throws IOException
    {
        String table = createTable("test_shredded_entries");
        assertThat(appendVariantFile(table, objectShredding(field("name", string())), ImmutableList.of(
                row(0, obj("name", "Alice")),
                row(1, null),
                row(5, obj("name", "Carol"))))).isEqualTo(2);

        // Iceberg's writer records a bound for the variant column too (field id 2). $files shows only the id column's;
        // $entries and $all_entries print the same: the primitive column's bounds and nothing for the variant
        assertThat(query("SELECT lower_bounds, upper_bounds FROM \"" + table + "$files\""))
                .matches("VALUES (CAST(ROW(0) AS ROW(\"1\" integer)), CAST(ROW(5) AS ROW(\"1\" integer)))");
        assertThat(query("SELECT data_file.lower_bounds, data_file.upper_bounds FROM \"" + table + "$entries\""))
                .matches("VALUES (MAP(ARRAY[1], ARRAY[VARCHAR '0']), MAP(ARRAY[1], ARRAY[VARCHAR '5']))");
        assertThat(query("SELECT data_file.lower_bounds, data_file.upper_bounds FROM \"" + table + "$all_entries\""))
                .matches("VALUES (MAP(ARRAY[1], ARRAY[VARCHAR '0']), MAP(ARRAY[1], ARRAY[VARCHAR '5']))");
        assertThat(query("SELECT record_count, file_count, data.id.min, data.id.max FROM \"" + table + "$partitions\""))
                .matches("VALUES (BIGINT '3', BIGINT '1', 0, 5)");
    }

    @Test
    void testUnexpectedVariantShapeStillFails()
    {
        // a variant group with a third child that is not typed_value is not a layout this reader knows
        MessageType unknownChild = new MessageType("table",
                variantGroup()
                        .required(BINARY).named("metadata")
                        .required(BINARY).named("value")
                        .optional(BINARY).named("extra")
                        .named("v"));
        assertThatThrownBy(() -> convert(unknownChild))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid VARIANT column, expected exactly 2 children but found: 3");

        // the same group with a typed_value takes the shredded path, which names the column when it refuses a layout
        MessageType shreddedWithExtraChild = new MessageType("table",
                variantGroup()
                        .required(BINARY).named("metadata")
                        .optional(BINARY).named("value")
                        .optional(BINARY).as(stringType()).named("typed_value")
                        .optional(BINARY).named("extra")
                        .named("v"));
        assertThatThrownBy(() -> convert(shreddedWithExtraChild))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Unsupported shredded VARIANT layout in column v");
    }

    @Test
    void testLayoutErrorsNameTheFile()
            throws IOException
    {
        // Iceberg's writer emits no invalid layout, so these files are written by parquet-java with the table's field ids
        String table = createTable("test_layout_error_file");
        // refused while the reader is built: a child beside value and typed_value
        String dataPath = appendRawFile(
                table,
                """
                message table {
                  required int32 id = 1;
                  optional group v (VARIANT(1)) = 2 {
                    required binary metadata;
                    optional binary value;
                    optional binary typed_value (STRING);
                    optional binary extra;
                  }
                }
                """,
                (_, row) -> row.addGroup("v").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0, 0})));
        assertThat(query("SELECT CAST(v AS JSON) FROM " + table))
                .failure()
                .hasMessage("Unsupported shredded VARIANT layout in column v: unexpected child 'extra' beside value and typed_value (file %s)".formatted(dataPath));

        // refused while the rows are read: metadata bytes that cannot be decoded
        String readTable = createTable("test_layout_error_file_read");
        String readPath = appendRawFile(
                readTable,
                """
                message table {
                  required int32 id = 1;
                  optional group v (VARIANT(1)) = 2 {
                    required binary metadata;
                    optional binary value;
                    optional binary typed_value (STRING);
                  }
                }
                """,
                (_, row) -> row.addGroup("v")
                        .append("metadata", Binary.fromConstantByteArray(new byte[] {(byte) 0xFF, (byte) 0xFF}))
                        .append("typed_value", "x"));
        assertThat(query("SELECT CAST(v AS JSON) FROM " + readTable))
                .failure()
                .hasMessageStartingWith("Unsupported shredded VARIANT layout in column v: invalid variant encoding in the file: ")
                .hasMessageEndingWith(" (file %s)".formatted(readPath));
        // the column is not read, so neither file fails
        assertThat(query("SELECT id FROM " + table)).matches("VALUES 0");
        assertThat(query("SELECT id FROM " + readTable)).matches("VALUES 0");
    }

    /**
     * Writes one row with parquet-java's example writer (id 0, the rest filled by {@code fill}) into the table's data
     * directory and commits it. Returns the file's path.
     */
    private String appendRawFile(String tableName, String schemaText, BiConsumer<MessageType, SimpleGroup> fill)
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        SimpleGroup row = new SimpleGroup(schema);
        row.append("id", 0);
        fill.accept(schema, row);
        Path directory = Files.createTempDirectory("raw-variant");
        try {
            Path file = directory.resolve("raw.parquet");
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                    .withType(schema)
                    .build()) {
                writer.write(row);
            }
            byte[] data = Files.readAllBytes(file);
            BaseTable table = loadTable(tableName);
            String dataPath = table.location() + "/data/raw-" + UUID.randomUUID() + ".parquet";
            getFileSystemFactory(getQueryRunner()).create(SESSION).newOutputFile(Location.of(dataPath)).createOrOverwrite(data);
            table.newAppend()
                    .appendFile(DataFiles.builder(table.spec())
                            .withPath(dataPath)
                            .withFormat(FileFormat.PARQUET)
                            .withFileSizeInBytes(data.length)
                            .withRecordCount(1)
                            .build())
                    .commit();
            return dataPath;
        }
        finally {
            deleteRecursively(directory, ALLOW_INSECURE);
        }
    }

    private static void convert(MessageType schema)
    {
        ColumnIO root = new ColumnIOFactory().getColumnIO(schema);
        ColumnIO variant = ((GroupColumnIO) root).getChild("v");
        IcebergParquetColumnIOConverter.constructField(
                new IcebergParquetColumnIOConverter.FieldContext(VARIANT, ColumnIdentity.primitiveColumnIdentity(1, "v")),
                variant);
    }

    private static GroupBuilder<GroupType> variantGroup()
    {
        return org.apache.parquet.schema.Types.optionalGroup().as(variantType((byte) 1));
    }

    // ---- writing ----

    private String createTable(String prefix)
    {
        return createTable(prefix, ID_AND_VARIANT);
    }

    private String createTable(String prefix, Schema schema)
    {
        String tableName = prefix + "_" + randomNameSuffix();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", tableName);
        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        schema,
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.<String, String>builder()
                                .put(FORMAT_VERSION, "3")
                                .put(DEFAULT_FILE_FORMAT, "parquet")
                                // small row groups, so that a file holds several
                                .put("write.parquet.row-group-size-bytes", "4096")
                                .put("write.parquet.row-group-check-min-record-count", "10")
                                .put("write.parquet.row-group-check-max-record-count", "10")
                                .buildOrThrow())
                .commitTransaction();
        return tableName;
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), "hive", "tpch");
    }

    /**
     * Writes one data file with Iceberg's writer and commits it. Returns the number of non-null values
     * the file holds in {@code typed_value} columns, which the caller asserts.
     */
    private long appendVariantFile(String tableName, VariantShreddingFunction shredding, List<Record> rows)
            throws IOException
    {
        return appendFile(tableName, shredding, rows);
    }

    private long appendFile(String tableName, VariantShreddingFunction shredding, List<Record> rows)
            throws IOException
    {
        Table table = loadTable(tableName);
        String dataPath = table.location() + "/data/data-" + UUID.randomUUID() + ".parquet";
        Parquet.DataWriteBuilder builder = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create);
        if (shredding != null) {
            builder.variantShreddingFunc(shredding);
        }
        try (DataWriter<Record> writer = builder.build()) {
            for (Record row : rows) {
                writer.write(row);
            }
            writer.close();
            table.newAppend().appendFile(writer.toDataFile()).commit();
            return typedValueCount(dataPath);
        }
    }

    private static Record row(int id, Object variant)
    {
        Record record = GenericRecord.create(ID_AND_VARIANT);
        record.setField("id", id);
        record.setField("v", variant == null ? null : asVariant(variant));
        return record;
    }

    private static Record nestedRow(Schema schema, Types.StructType struct, int id, Object variant, Integer n)
    {
        Record inner = GenericRecord.create(struct);
        inner.setField("v", variant == null ? null : asVariant(variant));
        inner.setField("n", n);
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("r", inner);
        return record;
    }

    private static Record collectionRow(Schema schema, int id, List<Object> array, Map<String, Object> map)
    {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        if (array != null) {
            List<Variant> elements = new ArrayList<>();
            for (Object element : array) {
                elements.add(element == null ? null : asVariant(element));
            }
            record.setField("a", elements);
        }
        if (map != null) {
            Map<String, Variant> values = new LinkedHashMap<>();
            map.forEach((key, value) -> values.put(key, value == null ? null : asVariant(value)));
            record.setField("m", values);
        }
        return record;
    }

    /**
     * A map in the order given; values may be null.
     */
    private static Map<String, Object> mapOf(Object... keyValuePairs)
    {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }

    private static Variant asVariant(Object value)
    {
        if (value instanceof Variant variant) {
            return variant;
        }
        // a bare value, a list, or an object built by obj(): the metadata holds every field name at any depth
        TreeSet<String> names = new TreeSet<>();
        collectFieldNames(value, names);
        VariantMetadata metadata = names.isEmpty() ? Variants.emptyMetadata() : Variants.metadata(names);
        return Variant.of(metadata, toValue(value, metadata));
    }

    private static void collectFieldNames(Object value, TreeSet<String> names)
    {
        if (value instanceof ObjectSpec spec) {
            names.addAll(spec.fields.keySet());
            spec.fields.values().forEach(fieldValue -> collectFieldNames(fieldValue, names));
        }
        else if (value instanceof List<?> list) {
            list.forEach(element -> collectFieldNames(element, names));
        }
    }

    private static VariantValue toValue(Object value, VariantMetadata metadata)
    {
        if (value instanceof ObjectSpec spec) {
            ShreddedObject object = Variants.object(metadata);
            spec.fields.forEach((name, fieldValue) -> object.put(name, toValue(fieldValue, metadata)));
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
            case Long longValue -> Variants.of(longValue);
            case Double doubleValue -> Variants.of(doubleValue);
            case Boolean bool -> Variants.of(bool);
            case BigDecimal decimal -> Variants.of(decimal);
            case ByteBuffer bytes -> Variants.of(bytes);
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

    private record ShreddedField(String name, Type typedValue) {}

    private static ShreddedField field(String name, Type typedValue)
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

    /**
     * A shredded list's typed_value: the three-level form, holding the given element group.
     */
    private static Type list(Type element)
    {
        return org.apache.parquet.schema.Types.optionalList().element(element).named("typed_value");
    }

    private static Type primitive(PrimitiveTypeName type, LogicalTypeAnnotation annotation)
    {
        PrimitiveBuilder<PrimitiveType> builder = org.apache.parquet.schema.Types.optional(type);
        if (type == FIXED_LEN_BYTE_ARRAY) {
            builder = builder.length(16);
        }
        if (annotation != null) {
            builder = builder.as(annotation);
        }
        return builder.named("typed_value");
    }

    private static Type string()
    {
        return primitive(BINARY, stringType());
    }

    private static Type int32()
    {
        return primitive(INT32, intType(32, true));
    }

    private static Type int64()
    {
        return primitive(INT64, intType(64, true));
    }

    private static Type doubleType()
    {
        return primitive(DOUBLE, null);
    }

    private static Type bool()
    {
        return primitive(BOOLEAN, null);
    }

    private static Type date()
    {
        return primitive(INT32, dateType());
    }

    private static Type uuid()
    {
        return primitive(FIXED_LEN_BYTE_ARRAY, uuidType());
    }

    private static Type timestamp(boolean adjustedToUtc)
    {
        return primitive(INT64, timestampType(adjustedToUtc, MICROS));
    }

    // ---- footer inspection ----

    private ParquetMetadata footer(String location)
    {
        return getParquetFileMetadata(getFileSystemFactory(getQueryRunner()).create(SESSION).newInputFile(Location.of(location)));
    }

    private long typedValueCount(String dataPath)
            throws IOException
    {
        long count = 0;
        for (BlockMetadata block : footer(dataPath).getBlocks()) {
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

    /**
     * Non-null values in typed_value columns that sit in an element of a shredded list, over the table's files.
     */
    private long typedValuesInLists(String tableName)
            throws IOException
    {
        BaseTable table = loadTable(tableName);
        long count = 0;
        for (DataFile file : SnapshotChanges.builderFor(table).build().addedDataFiles()) {
            for (BlockMetadata block : footer(file.location()).getBlocks()) {
                for (ColumnChunkMetadata chunk : block.columns()) {
                    List<String> path = List.of(chunk.getPath().toArray());
                    if (path.getLast().equals("typed_value") && path.contains("list") && path.contains("element")) {
                        count += chunk.getValueCount() - chunk.getStatistics().getNumNulls();
                    }
                }
            }
        }
        return count;
    }

    private List<Long> rowGroupStartIds(String tableName)
            throws IOException
    {
        BaseTable table = loadTable(tableName);
        ImmutableList.Builder<Long> starts = ImmutableList.builder();
        for (DataFile file : SnapshotChanges.builderFor(table).build().addedDataFiles()) {
            long rows = 0;
            for (BlockMetadata block : footer(file.location()).getBlocks()) {
                starts.add(rows);
                rows += block.rowCount();
            }
        }
        return starts.build();
    }

    private int rowGroupCount(String tableName)
            throws IOException
    {
        BaseTable table = loadTable(tableName);
        int blocks = 0;
        for (DataFile file : SnapshotChanges.builderFor(table).build().addedDataFiles()) {
            blocks += footer(file.location()).getBlocks().size();
        }
        return blocks;
    }
}
