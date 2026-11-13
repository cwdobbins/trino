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
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.variant.Variant;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.VariantJsonParser;
import org.apache.parquet.variant.VariantValueWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertSameAsUnshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.createTempDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.dataPageCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.deleteDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readColumn;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.rowGroupCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.shredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.toJson;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.typedValueCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.unshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeAndRead;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeRecords;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Awkward nesting and file shapes: the shredded reader against the unshredded layout of the same rows.
 * Integers in JSON parse as the smallest width, so integer columns are declared int32 (INTEGER(8,true)).
 */
final class TestShreddedVariantLayouts
{
    private static final String STRING_FIELD = "{ optional binary value; optional binary typed_value (STRING); }";
    private static final String INT8_FIELD = "{ optional binary value; optional int32 typed_value (INTEGER(8,true)); }";

    @Test
    void testNestedObjectsInArraysInObjects()
    {
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional group typed_value {
                      required group items {
                        optional binary value;
                        optional group typed_value (LIST) {
                          repeated group list {
                            required group element {
                              optional binary value;
                              optional group typed_value {
                                required group k %s
                                required group v %s
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """.formatted(STRING_FIELD, INT8_FIELD));
        assertSameAsUnshredded(schema, Arrays.asList(
                "{\"a\":{\"items\":[{\"k\":\"x\",\"v\":1},{\"k\":\"y\"}]}}",
                "{\"a\":{\"items\":[]}}",
                "{\"a\":{\"items\":[{\"k\":\"z\",\"v\":300},5,{\"k\":\"w\",\"extra\":true},null,{}]}}",
                "{\"a\":{\"items\":null}}",
                "{\"a\":{\"items\":\"not a list\"}}",
                "{\"a\":{\"other\":1}}",
                "{\"a\":\"not an object\"}",
                "{\"a\":null}",
                "{\"b\":1}",
                "{}",
                null,
                "[1]",
                "{\"a\":{\"items\":[{\"k\":\"last\",\"v\":127}]},\"b\":{\"c\":[1,2]}}"));
    }

    @Test
    void testFieldValueFollowedByOtherFields()
    {
        // REGRESSION: parquet-java's variant writer writes a shredded object field's value as a view of its parent's
        // buffer that runs on past the field's own end, so meta.value here is the object {"deep":[1,{"x":2}]} followed
        // by the bytes of the fields that come after it. Taken as the value's length, those bytes broke remapping its field ids.
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group tags {
                    optional binary value;
                    optional group typed_value (LIST) {
                      repeated group list {
                        required group element %s
                      }
                    }
                  }
                  required group meta { optional binary value; }
                }
                """.formatted(STRING_FIELD));
        List<Variant> read = writeAndRead(schema, ImmutableList.of("{\"tags\":[\"a\",\"b\"],\"other\":1,\"meta\":{\"deep\":[1,{\"x\":2}]}}"));
        assertThat(read).hasSize(1);
        assertThat(toJson(read.get(0))).isEqualTo("{\"meta\":{\"deep\":[1,{\"x\":2}]},\"other\":1,\"tags\":[\"a\",\"b\"]}");
    }

    @Test
    void testShreddedOnlyAtInnerLevel()
    {
        // tags is shredded as a list of strings; meta has only a value column; everything else lives in the outer value
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group tags {
                    optional binary value;
                    optional group typed_value (LIST) {
                      repeated group list {
                        required group element %s
                      }
                    }
                  }
                  required group meta { optional binary value; }
                }
                """.formatted(STRING_FIELD));
        assertSameAsUnshredded(schema, Arrays.asList(
                "{\"tags\":[\"a\",\"b\"],\"other\":1,\"meta\":{\"deep\":[1,{\"x\":2}]}}",
                "{\"tags\":[],\"other\":{\"nested\":true}}",
                "{\"tags\":[\"c\",1,null,\"d\"],\"meta\":7}",
                "{\"tags\":\"nope\"}",
                "{\"other\":\"only\"}",
                "{\"meta\":null}",
                null,
                "\"scalar\"",
                "{\"tags\":[\"z\"]}"));
    }

    @Test
    void testEmptyObjectAndEmptyArray()
    {
        MessageType objectSchema = shredded(
                """
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional group typed_value {
                      required group x %s
                    }
                  }
                  required group l {
                    optional binary value;
                    optional group typed_value (LIST) {
                      repeated group list {
                        required group element %s
                      }
                    }
                  }
                }
                """.formatted(STRING_FIELD, STRING_FIELD));
        assertSameAsUnshredded(objectSchema, Arrays.asList(
                "{}",
                "{\"a\":{},\"l\":[]}",
                "{\"a\":{\"x\":\"s\"},\"l\":[\"q\"]}",
                "{\"a\":{}}",
                "{\"l\":[]}",
                "{\"a\":[],\"l\":{}}",
                "{\"a\":{},\"l\":[],\"extra\":{}}",
                "{\"extra\":[]}"));

        MessageType arraySchema = shredded(
                """
                optional group typed_value (LIST) {
                  repeated group list {
                    required group element {
                      optional binary value;
                      optional group typed_value (LIST) {
                        repeated group list {
                          required group element %s
                        }
                      }
                    }
                  }
                }
                """.formatted(STRING_FIELD));
        assertSameAsUnshredded(arraySchema, Arrays.asList(
                "[]",
                "[[]]",
                "[[],[\"a\"],[]]",
                "[[\"a\",\"b\"],[],{}]",
                "[{},[]]",
                "{}",
                "[]"));
    }

    @Test
    void testNothingActuallyShredded()
    {
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group a %s
                  required group b %s
                }
                """.formatted(INT8_FIELD, STRING_FIELD));
        // a is declared int8 and b a string, but every value is something else or missing
        assertSameAsUnshredded(
                schema,
                Arrays.asList(
                        "{\"a\":\"x\"}",
                        "{\"a\":1.5,\"b\":true}",
                        "{\"a\":300,\"b\":[1]}",
                        "{\"b\":{\"c\":1}}",
                        "{\"c\":1}",
                        "{}",
                        "\"s\"",
                        "[1]",
                        "7",
                        null),
                false);
    }

    @Test
    void testKeysDifferingOnlyByCase()
    {
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group a %s
                  required group A %s
                }
                """.formatted(INT8_FIELD, INT8_FIELD));
        assertSameAsUnshredded(schema, Arrays.asList(
                "{\"a\":1,\"A\":2}",
                "{\"a\":1}",
                "{\"A\":2}",
                "{\"a\":\"x\",\"A\":3}",
                "{\"a\":4,\"A\":\"y\",\"b\":5}",
                "{\"B\":1,\"b\":2}",
                "{}"));
        // the values stay with their own key
        List<Variant> read = writeAndRead(schema, ImmutableList.of("{\"a\":1,\"A\":2}"));
        assertThat(toJson(read.get(0))).isEqualTo("{\"A\":2,\"a\":1}");

        // REGRESSION: the footer lowercases column names, which turned the shredded field eventType into eventtype
        MessageType camelCase = objectSchemaWithFields(ImmutableList.of("eventType", "Ключ"), PrimitiveTypeName.INT32);
        assertSameAsUnshredded(camelCase, Arrays.asList("{\"eventType\":1,\"Ключ\":2}", "{\"eventType\":3}", "{\"eventtype\":4,\"ключ\":5}"));
        assertThat(toJson(writeAndRead(camelCase, ImmutableList.of("{\"eventType\":1,\"Ключ\":2}")).get(0))).isEqualTo("{\"eventType\":1,\"Ключ\":2}");
    }

    @Test
    void testKeysWithDotsAndNonAscii()
    {
        MessageType schema = objectSchemaWithFields(ImmutableList.of("a.b", "ключ", "a", "b"), PrimitiveTypeName.INT32);
        assertSameAsUnshredded(schema, Arrays.asList(
                "{\"a.b\":1,\"ключ\":2}",
                "{\"a.b\":1}",
                "{\"ключ\":2,\"other\":\"x\"}",
                "{\"a\":{\"b\":3},\"b\":4}",
                "{\"a.b\":\"s\",\"ключ\":\"ю\"}",
                "{\"ключ\":127,\"a\":1,\"b\":2,\"a.b\":3}",
                "{}"));
    }

    @Test
    void testWideObject()
    {
        int fields = 300;
        StringBuilder typedFields = new StringBuilder();
        for (int i = 0; i < fields; i++) {
            typedFields.append("required group f%s %s\n".formatted(i, INT8_FIELD));
        }
        MessageType schema = shredded("optional group typed_value {\n" + typedFields + "}");

        List<String> rows = new ArrayList<>();
        for (int row = 0; row < 40; row++) {
            StringBuilder json = new StringBuilder("{");
            boolean first = true;
            for (int i = 0; i < fields; i++) {
                if ((i + row) % (row % 5 + 1) == 1 && row % 3 != 0) {
                    continue; // vary which fields are present
                }
                json.append(first ? "" : ",").append("\"f%s\":".formatted(i));
                json.append(row % 7 == 6 && i % 50 == 0 ? "\"s%s\"".formatted(i) : String.valueOf((i + row) % 100)); // some fall back to value
                first = false;
            }
            if (row % 4 == 0) {
                json.append(first ? "" : ",").append("\"extra%s\":%s".formatted(row, row)); // unshredded field
            }
            json.append("}");
            rows.add(json.toString());
        }
        rows.add(null);
        rows.add("{}");
        assertSameAsUnshredded(schema, rows);
    }

    @Test
    void testLargeString()
    {
        String large = "0123456789abcdefghijklmnopqrstuvwxyz-".repeat(4 * 1024 * 1024 / 37 + 1).substring(0, 4 * 1024 * 1024);
        assertThat(large).hasSize(4 * 1024 * 1024);
        List<String> rows = Arrays.asList("\"small\"", "\"" + large + "\"", null, "\"after\"", "7");
        assertSameAsUnshredded(shredded("optional binary typed_value (STRING);"), rows);

        // and as a field of an object, next to a small one
        MessageType objectSchema = shredded(
                """
                optional group typed_value {
                  required group s %s
                  required group t %s
                }
                """.formatted(STRING_FIELD, STRING_FIELD));
        assertSameAsUnshredded(objectSchema, Arrays.asList(
                "{\"s\":\"" + large + "\",\"t\":\"small\"}",
                "{\"s\":\"before\"}",
                "{\"t\":\"" + large + "\"}"));
    }

    @Test
    void testManyRowGroupsAndPages()
            throws IOException
    {
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group n %s
                  required group s %s
                }
                """.formatted(INT8_FIELD, STRING_FIELD));
        int rowCount = 50_000;
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            if (i % 7 == 0) {
                rows.add(null);
            }
            else if (i % 11 == 0) {
                rows.add("{\"n\":\"not a number\",\"s\":%s}".formatted(i));
            }
            else if (i % 13 == 0) {
                rows.add(i % 2 == 0 ? "\"scalar %s\"".formatted(i) : "[%s]".formatted(i));
            }
            else if (i % 5 == 0) {
                rows.add("{\"n\":%s,\"s\":\"row %s\",\"extra\":{\"i\":%s}}".formatted(i % 100, i, i));
            }
            else {
                rows.add("{\"n\":%s,\"s\":\"row %s\"}".formatted(i % 100, i));
            }
        }

        Path directory = createTempDirectory();
        try {
            List<org.apache.parquet.variant.Variant> variants = new ArrayList<>();
            for (String json : rows) {
                variants.add(json == null ? null : VariantJsonParser.parseJson(json));
            }
            File shreddedFile = writeVariantFile(directory, "shredded.parquet", schema, variants, 64 * 1024, 4 * 1024);
            File plainFile = writeVariantFile(directory, "plain.parquet", unshredded(), variants, 64 * 1024, 4 * 1024);

            assertThat(rowGroupCount(shreddedFile)).as("row groups").isGreaterThan(5);
            assertThat(dataPageCount(shreddedFile, "v", "typed_value", "s", "typed_value")).as("data pages of v.typed_value.s.typed_value").isGreaterThan(50);
            assertThat(typedValueCount(shreddedFile)).as("typed values written").isGreaterThan(rowCount);

            List<Variant> expected = readFile(plainFile);
            assertThat(expected).hasSize(rowCount);
            // batch sizes that divide neither a page's rows nor a row group's
            for (int batchSize : new int[] {997, 4093, 64, 31_337}) {
                assertLogicallyEqual(readFile(shreddedFile, batchSize), expected, rows);
            }
            assertLogicallyEqual(readFile(shreddedFile), expected, rows);
        }
        finally {
            deleteDirectory(directory);
        }
    }

    @Test
    void testShreddedArrayOfObjectsAcrossRowGroupsAndPages()
            throws IOException
    {
        // An array of objects holding an array, shredded at every level, across many row groups and pages, with SQL NULL rows,
        // non-object rows, missing and empty arrays and elements that fall back to value interleaved. A SQL NULL or a
        // non-object row makes the reader wrap the struct's children in dictionary blocks, so every batch exercises the
        // position mapping through those blocks at each array level.
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group id %s
                  required group items {
                    optional binary value;
                    optional group typed_value (LIST) {
                      repeated group list {
                        required group element {
                          optional binary value;
                          optional group typed_value {
                            required group k %s
                            required group n %s
                            required group tags {
                              optional binary value;
                              optional group typed_value (LIST) {
                                repeated group list {
                                  required group element %s
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """.formatted(INT8_FIELD, STRING_FIELD, INT8_FIELD, STRING_FIELD));
        int rowCount = 30_000;
        List<String> rows = new ArrayList<>();
        // counted from the rules below, independently of the writer: one for each string or int8 that lands in a typed column
        long expectedTypedValues = 0;
        for (int i = 0; i < rowCount; i++) {
            int id = i % 100;
            if (i % 7 == 0) {
                rows.add(null);
            }
            else if (i % 11 == 0) {
                // not an object, so the whole row falls back to the top-level value
                rows.add(i % 2 == 0 ? "\"scalar %s\"".formatted(i) : "[%s,{\"items\":[]}]".formatted(id));
            }
            else if (i % 13 == 0) {
                // items missing
                rows.add("{\"id\":%s,\"other\":%s}".formatted(id, i));
                expectedTypedValues++;
            }
            else if (i % 17 == 0) {
                rows.add("{\"id\":%s,\"items\":[]}".formatted(id));
                expectedTypedValues++;
            }
            else if (i % 19 == 0) {
                // items is not a list, so it falls back to its value
                rows.add("{\"id\":%s,\"items\":\"not a list %s\"}".formatted(id, i));
                expectedTypedValues++;
            }
            else {
                List<String> elements = new ArrayList<>();
                for (int j = 0; j < (i % 4) + 1; j++) {
                    int n = (i + j) % 100;
                    switch (j) {
                        case 0 -> {
                            List<String> tags = new ArrayList<>();
                            for (int t = 0; t < (i + j) % 3; t++) {
                                tags.add("\"t%s\"".formatted(t));
                            }
                            elements.add("{\"k\":\"r%s_%s\",\"n\":%s,\"tags\":[%s]}".formatted(i, j, n, String.join(",", tags)));
                            expectedTypedValues += 2 + tags.size();
                        }
                        case 1 -> {
                            // a tag that is not a string falls back to its element's value; an extra field goes to the object's value
                            elements.add("{\"k\":\"r%s_%s\",\"n\":%s,\"tags\":[\"a\",%s],\"extra\":true}".formatted(i, j, n, n));
                            expectedTypedValues += 3;
                        }
                        // an element that is not an object: a variant null, or a number
                        case 2 -> elements.add(i % 3 == 0 ? "null" : String.valueOf(n));
                        case 3 -> {
                            // n is not an int8, so it falls back to its value; tags missing
                            elements.add("{\"k\":\"e%s\",\"n\":\"str\"}".formatted(i));
                            expectedTypedValues++;
                        }
                        default -> throw new IllegalStateException("unexpected element " + j);
                    }
                }
                rows.add("{\"id\":%s,\"items\":[%s]}".formatted(id, String.join(",", elements)));
                expectedTypedValues++;
            }
        }

        Path directory = createTempDirectory();
        try {
            List<org.apache.parquet.variant.Variant> variants = new ArrayList<>();
            for (String json : rows) {
                variants.add(json == null ? null : VariantJsonParser.parseJson(json));
            }
            File shreddedFile = writeVariantFile(directory, "shredded.parquet", schema, variants, 64 * 1024, 4 * 1024);
            File plainFile = writeVariantFile(directory, "plain.parquet", unshredded(), variants, 64 * 1024, 4 * 1024);

            assertThat(rowGroupCount(shreddedFile)).as("row groups").isGreaterThan(5);
            assertThat(dataPageCount(shreddedFile, "v", "typed_value", "items", "typed_value", "list", "element", "typed_value", "k", "typed_value"))
                    .as("data pages of v.typed_value.items.typed_value.list.element.typed_value.k.typed_value")
                    .isGreaterThan(50);
            assertThat(typedValueCount(shreddedFile)).as("typed values written").isEqualTo(expectedTypedValues);

            List<Variant> expected = readFile(plainFile);
            assertThat(expected).hasSize(rowCount);
            // batch sizes that divide neither a page's rows nor a row group's
            for (int batchSize : new int[] {997, 4093, 64, 31_337}) {
                assertLogicallyEqual(readFile(shreddedFile, batchSize), expected, rows);
            }
            assertLogicallyEqual(readFile(shreddedFile), expected, rows);
        }
        finally {
            deleteDirectory(directory);
        }
    }

    @Test
    void testZeroRows()
    {
        // no rows, so no typed values: the check is that an empty shredded file reads as empty
        assertSameAsUnshredded(shredded("optional binary typed_value (STRING);"), ImmutableList.of(), false);
        assertThat(writeAndRead(shredded("optional binary typed_value (STRING);"), ImmutableList.of())).isEmpty();
    }

    @Test
    void testAllRowsSqlNull()
    {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            rows.add(null);
        }
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group a %s
                }
                """.formatted(INT8_FIELD));
        // no value is written at all, so no typed value either
        assertSameAsUnshredded(schema, rows, false);
        assertThat(writeAndRead(schema, rows)).hasSize(5000).containsOnlyNulls();
    }

    private static final String NESTED_VARIANT =
            """
            optional group %s (VARIANT(1)) {
              required binary metadata;
              optional binary value;
              optional group typed_value {
                required group n %s
                required group s %s
              }
            }
            """.formatted("%s", INT8_FIELD, STRING_FIELD);

    private static final MessageType NESTED_SHREDDED = MessageTypeParser.parseMessageType(nestedSchema(NESTED_VARIANT));

    private static final MessageType NESTED_UNSHREDDED = MessageTypeParser.parseMessageType(nestedSchema(
            """
            optional group %s (VARIANT(1)) {
              required binary metadata;
              required binary value;
            }
            """));

    private static String nestedSchema(String variantTemplate)
    {
        return """
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
                 optional group m (MAP) {
                   repeated group key_value {
                     required binary key (STRING);
                     %s
                   }
                 }
               }
               """.formatted(variantTemplate.formatted("v"), variantTemplate.formatted("element"), variantTemplate.formatted("value"));
    }

    // One input row of the nested file: a null r, a, or m is a null row/array/map; a null variant json is an absent variant group
    private record NestedRow(Integer id, boolean rowPresent, String rowVariant, List<String> array, Map<String, String> map) {}

    @Test
    void testVariantNestedInRowArrayAndMap()
    {
        String[] documents = {
                "{\"n\":1,\"s\":\"one\"}",
                "{\"n\":2,\"s\":\"two\",\"x\":[1,2]}",
                "{\"n\":\"no\",\"s\":3}",
                "\"scalar\"",
                "{}",
                "[1,2,3]",
                "null",
                "{\"n\":127}",
        };
        List<NestedRow> rows = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            String document = documents[i % documents.length];
            // null parent, empty container, null variant inside a non-null parent, and ordinary rows around them
            boolean rowPresent = i % 9 != 4;
            String rowVariant = i % 10 == 3 ? null : document;
            List<String> array = switch (i % 8) {
                case 0 -> null;
                case 1 -> ImmutableList.of();
                case 2 -> Arrays.asList(document, null, documents[(i + 1) % documents.length]);
                case 3 -> Arrays.asList((String) null);
                default -> ImmutableList.of(document, documents[(i + 3) % documents.length]);
            };
            Map<String, String> map = switch (i % 6) {
                case 0 -> null;
                case 1 -> mapOf();
                case 2 -> mapOf("k" + i, document, "other", null);
                case 3 -> mapOf("only", null);
                default -> mapOf("a", document, "b", documents[(i + 2) % documents.length]);
            };
            rows.add(new NestedRow(i % 17 == 0 ? null : i, rowPresent, rowVariant, array, map));
        }

        Path directory = createTempDirectory();
        try {
            File shreddedFile = writeRecords(directory, "shredded.parquet", NESTED_SHREDDED, nestedRecords(NESTED_SHREDDED, rows));
            File plainFile = writeRecords(directory, "plain.parquet", NESTED_UNSHREDDED, nestedRecords(NESTED_UNSHREDDED, rows));
            assertThat(typedValueCount(shreddedFile)).as("typed values written").isGreaterThan(rows.size());

            Type rowType = RowType.rowType(RowType.field("id", INTEGER), RowType.field("v", VARIANT));
            Type arrayType = new ArrayType(VARIANT);
            Type mapType = new MapType(VARCHAR, VARIANT, new TypeOperators());

            for (int batchSize : new int[] {1024, 13}) {
                for (Map.Entry<String, Type> column : Map.of("r", rowType, "a", arrayType, "m", mapType).entrySet()) {
                    String name = column.getKey();
                    Type type = column.getValue();
                    List<Object> actual = readColumn(shreddedFile, name, type, batchSize, (block, position) -> toObject(type, block, position, variant -> variant));
                    List<Object> expected = readColumn(plainFile, name, type, batchSize, (block, position) -> toObject(type, block, position, variant -> variant));
                    assertThat(actual).as("column %s, batch size %s", name, batchSize).isEqualTo(expected);

                    // the JSON text of every variant, which Variant equality does not compare
                    List<Object> actualJson = readColumn(shreddedFile, name, type, batchSize, (block, position) -> toObject(type, block, position, TestShreddedVariantLayouts::jsonOrNull));
                    List<Object> expectedJson = readColumn(plainFile, name, type, batchSize, (block, position) -> toObject(type, block, position, TestShreddedVariantLayouts::jsonOrNull));
                    assertThat(actualJson).as("column %s JSON, batch size %s", name, batchSize).isEqualTo(expectedJson);

                    // where nulls and sizes fall comes from the input rows, not from the unshredded read
                    List<Object> shape = readColumn(shreddedFile, name, type, batchSize, (block, position) -> toObject(type, block, position, variant -> variant == null ? null : "variant"));
                    assertThat(shape).as("column %s shape, batch size %s", name, batchSize).isEqualTo(rows.stream().map(row -> expectedShape(name, row)).toList());
                }
            }
        }
        finally {
            deleteDirectory(directory);
        }
    }

    private static Object expectedShape(String column, NestedRow row)
    {
        return switch (column) {
            case "r" -> {
                if (!row.rowPresent()) {
                    yield null;
                }
                List<Object> fields = new ArrayList<>();
                fields.add(row.id());
                fields.add(row.rowVariant() == null ? null : "variant");
                yield fields;
            }
            case "a" -> {
                if (row.array() == null) {
                    yield null;
                }
                List<Object> elements = new ArrayList<>();
                for (String element : row.array()) {
                    elements.add(element == null ? null : "variant");
                }
                yield elements;
            }
            default -> {
                if (row.map() == null) {
                    yield null;
                }
                Map<String, Object> entries = new LinkedHashMap<>();
                row.map().forEach((key, value) -> entries.put(key, value == null ? null : "variant"));
                yield entries;
            }
        };
    }

    private static Object jsonOrNull(Variant variant)
    {
        return variant == null ? null : toJson(variant);
    }

    // Variants become leaf(variant), rows and arrays lists, maps ordered maps; a null block position is null
    private static Object toObject(Type type, Block block, int position, Function<Variant, Object> leaf)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type == VARIANT) {
            return leaf.apply((Variant) VARIANT.getObject(block, position));
        }
        if (type instanceof RowType rowType) {
            SqlRow row = rowType.getObject(block, position);
            List<Object> fields = new ArrayList<>();
            for (int i = 0; i < rowType.getFields().size(); i++) {
                fields.add(toObject(rowType.getFields().get(i).getType(), row.getRawFieldBlock(i), row.getRawIndex(), leaf));
            }
            return fields;
        }
        if (type instanceof ArrayType arrayType) {
            Block elements = arrayType.getObject(block, position);
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < elements.getPositionCount(); i++) {
                values.add(toObject(arrayType.getElementType(), elements, i, leaf));
            }
            return values;
        }
        if (type instanceof MapType mapType) {
            SqlMap map = mapType.getObject(block, position);
            Map<String, Object> values = new LinkedHashMap<>();
            for (int i = 0; i < map.getSize(); i++) {
                Slice key = VARCHAR.getSlice(map.getRawKeyBlock(), map.getRawOffset() + i);
                values.put(key.toStringUtf8(), toObject(mapType.getValueType(), map.getRawValueBlock(), map.getRawOffset() + i, leaf));
            }
            return values;
        }
        if (type == INTEGER) {
            return INTEGER.getInt(block, position);
        }
        throw new IllegalArgumentException("unexpected type " + type);
    }

    private static Map<String, String> mapOf(String... keysAndValues)
    {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    private static List<Consumer<RecordConsumer>> nestedRecords(MessageType schema, List<NestedRow> rows)
    {
        GroupType rowGroup = schema.getType("r").asGroupType();
        GroupType variantInRow = rowGroup.getType("v").asGroupType();
        GroupType variantInArray = schema.getType("a").asGroupType().getType("list").asGroupType().getType("element").asGroupType();
        GroupType variantInMap = schema.getType("m").asGroupType().getType("key_value").asGroupType().getType("value").asGroupType();

        List<Consumer<RecordConsumer>> records = new ArrayList<>();
        for (NestedRow row : rows) {
            records.add(consumer -> {
                if (row.rowPresent()) {
                    consumer.startField("r", 0);
                    consumer.startGroup();
                    if (row.id() != null) {
                        consumer.startField("id", 0);
                        consumer.addInteger(row.id());
                        consumer.endField("id", 0);
                    }
                    if (row.rowVariant() != null) {
                        consumer.startField("v", 1);
                        VariantValueWriter.write(consumer, variantInRow, parse(row.rowVariant()));
                        consumer.endField("v", 1);
                    }
                    consumer.endGroup();
                    consumer.endField("r", 0);
                }
                if (row.array() != null) {
                    consumer.startField("a", 1);
                    consumer.startGroup();
                    if (!row.array().isEmpty()) {
                        consumer.startField("list", 0);
                        for (String element : row.array()) {
                            consumer.startGroup();
                            if (element != null) {
                                consumer.startField("element", 0);
                                VariantValueWriter.write(consumer, variantInArray, parse(element));
                                consumer.endField("element", 0);
                            }
                            consumer.endGroup();
                        }
                        consumer.endField("list", 0);
                    }
                    consumer.endGroup();
                    consumer.endField("a", 1);
                }
                if (row.map() != null) {
                    consumer.startField("m", 2);
                    consumer.startGroup();
                    if (!row.map().isEmpty()) {
                        consumer.startField("key_value", 0);
                        for (Map.Entry<String, String> entry : row.map().entrySet()) {
                            consumer.startGroup();
                            consumer.startField("key", 0);
                            consumer.addBinary(Binary.fromString(entry.getKey()));
                            consumer.endField("key", 0);
                            if (entry.getValue() != null) {
                                consumer.startField("value", 1);
                                VariantValueWriter.write(consumer, variantInMap, parse(entry.getValue()));
                                consumer.endField("value", 1);
                            }
                            consumer.endGroup();
                        }
                        consumer.endField("key_value", 0);
                    }
                    consumer.endGroup();
                    consumer.endField("m", 2);
                }
            });
        }
        return records;
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

    // A variant whose typed_value is an object of int8 fields with the given names (names parse badly as schema text)
    private static MessageType objectSchemaWithFields(List<String> names, PrimitiveTypeName physicalType)
    {
        Types.GroupBuilder<GroupType> typedValue = Types.optionalGroup();
        for (String name : names) {
            typedValue.addField(Types.requiredGroup()
                    .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
                    .addField(Types.optional(physicalType).as(LogicalTypeAnnotation.intType(8, true)).named("typed_value"))
                    .named(name));
        }
        GroupType variant = Types.optionalGroup()
                .as(LogicalTypeAnnotation.variantType((byte) 1))
                .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
                .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
                .addField(typedValue.named("typed_value"))
                .named("v");
        return Types.buildMessage().addField(variant).named("m");
    }
}
