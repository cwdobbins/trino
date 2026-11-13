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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ShreddedLayoutException;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertSameAsUnshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertUnsupportedLayout;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.shredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.typedValueCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.unshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeAndRead;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeFile;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.type.JsonType.JSON;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestShreddedVariantParquetReader
{
    private static final MessageType SHREDDED_SCHEMA = MessageTypeParser.parseMessageType(
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
                  required group b {
                    optional binary value;
                    optional binary typed_value (STRING);
                  }
                }
              }
            }
            """);

    private static final MessageType SCALAR_SCHEMA = MessageTypeParser.parseMessageType(
            """
            message m {
              optional group v (VARIANT(1)) {
                required binary metadata;
                optional binary value;
                optional group typed_value {
                  required group i8 { optional binary value; optional int32 typed_value (INTEGER(8,true)); }
                  required group dt { optional binary value; optional int32 typed_value (DATE); }
                  required group fl { optional binary value; optional float typed_value; }
                  required group dec { optional binary value; optional int32 typed_value (DECIMAL(5,2)); }
                }
              }
            }
            """);

    @Test
    void testReadShreddedVariant(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        // row 0: fully shredded object {a: 5, b: "hi"}
        SimpleGroup row0 = new SimpleGroup(SHREDDED_SCHEMA);
        Group v0 = row0.addGroup("v");
        v0.append("metadata", metadataBinary());
        Group typedValue0 = v0.addGroup("typed_value");
        typedValue0.addGroup("a").append("typed_value", 5L);
        typedValue0.addGroup("b").append("typed_value", Binary.fromString("hi"));

        // row 1: null variant (optional group absent)
        SimpleGroup row1 = new SimpleGroup(SHREDDED_SCHEMA);

        // row 2: partially shredded object: a from typed_value, c carried in value; b missing
        Variant partial = Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.ofLong(3)));
        SimpleGroup row2 = new SimpleGroup(SHREDDED_SCHEMA);
        Group v2 = row2.addGroup("v");
        v2.append("metadata", Binary.fromConstantByteArray(partial.metadata().toSlice().getBytes()));
        v2.append("value", Binary.fromConstantByteArray(partial.data().getBytes()));
        Group typedValue2 = v2.addGroup("typed_value");
        typedValue2.addGroup("a").append("typed_value", 1L);
        typedValue2.addGroup("b");

        List<Object> variants = writeAndReadVariants(SHREDDED_SCHEMA, tempDir, ImmutableList.of(row0, row1, row2));
        assertThat(variants).containsExactly(
                ImmutableMap.of("a", 5L, "b", "hi"),
                null,
                ImmutableMap.of("a", 1L, "c", 3L));
    }

    @Test
    void testReadShreddedScalarTypes(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        SimpleGroup row = new SimpleGroup(SCALAR_SCHEMA);
        Group v = row.addGroup("v");
        v.append("metadata", metadataBinary());
        Group typedValue = v.addGroup("typed_value");
        typedValue.addGroup("i8").append("typed_value", 7);
        typedValue.addGroup("dt").append("typed_value", toIntExact(LocalDate.of(2021, 2, 3).toEpochDay()));
        typedValue.addGroup("fl").append("typed_value", 1.5f);
        typedValue.addGroup("dec").append("typed_value", 123); // unscaled value of DECIMAL(5,2) 1.23

        List<Object> variants = writeAndReadVariants(SCALAR_SCHEMA, tempDir, ImmutableList.of(row));
        assertThat(variants).containsExactly(ImmutableMap.of(
                "i8", (byte) 7,
                "dt", LocalDate.of(2021, 2, 3),
                "fl", 1.5f,
                "dec", new BigDecimal("1.23")));
    }

    @Test
    void testShreddedObjectMatchesUnshredded()
    {
        assertSameAsUnshredded(
                shredded(
                        """
                        optional group typed_value {
                          required group name { optional binary value; optional binary typed_value (STRING); }
                          required group age  { optional binary value; optional int32 typed_value; }
                        }"""),
                Arrays.asList(
                        "{\"name\":\"a\",\"age\":1}",
                        "{\"name\":\"b\",\"age\":\"old\",\"extra\":true}",
                        "{\"other\":[1,2]}",
                        "\"not an object\"",
                        null));
    }

    private static final MessageType OBJECT_SCHEMA = shredded(
            """
            optional group typed_value {
              required group name { optional binary value; optional binary typed_value (STRING); }
              required group age  { optional binary value; optional int32 typed_value; }
            }""");

    private static final List<String> OBJECT_ROWS = Arrays.asList(
            "{\"name\":\"a\",\"age\":1}",
            "{\"name\":\"b\",\"age\":\"old\",\"extra\":true}",
            "{\"other\":[1,2]}",
            "\"not an object\"",
            null);

    @Test
    void testHarnessDetectsShreddedColumns(@TempDir java.nio.file.Path tempDir)
    {
        // typed values: name "a" and "b" (rows 0, 1) = 2. The JSON parser reads 1 as an int8, which does not fit age's
        // int32 column, so it falls back to value, as do age "old" and the other rows.
        assertThat(typedValueCount(writeFile(tempDir, "shredded.parquet", OBJECT_SCHEMA, OBJECT_ROWS))).isEqualTo(2L);
        // an int8 typed column does take the 1, so age adds one typed value
        MessageType withInt8Age = shredded(
                """
                optional group typed_value {
                  required group name { optional binary value; optional binary typed_value (STRING); }
                  required group age  { optional binary value; optional int32 typed_value (INTEGER(8,true)); }
                }""");
        assertThat(typedValueCount(writeFile(tempDir, "int8.parquet", withInt8Age, OBJECT_ROWS))).isEqualTo(3L);
        assertThat(typedValueCount(writeFile(tempDir, "plain.parquet", unshredded(), OBJECT_ROWS))).isEqualTo(0);
        // a layout whose typed columns never match the data is reported when typed values are expected
        assertThatThrownBy(() -> assertSameAsUnshredded(OBJECT_SCHEMA, Arrays.asList("\"x\"", "[1]")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("expected to be shredded");
        assertSameAsUnshredded(OBJECT_SCHEMA, Arrays.asList("\"x\"", "[1]"), false);
    }

    @Test
    void testOracleRejectsDifferences()
    {
        List<String> rows = Arrays.asList("{\"a\":1}", "{\"a\":2}");
        List<Variant> expected = writeAndRead(unshredded(), rows);
        List<Variant> other = writeAndRead(unshredded(), Arrays.asList("{\"a\":1}", "{\"a\":3}"));
        assertLogicallyEqual(expected, expected, rows);

        assertThatThrownBy(() -> assertLogicallyEqual(other, expected, rows)).isInstanceOf(AssertionError.class);

        List<Variant> withNull = new ArrayList<>(expected);
        withNull.set(1, null);
        assertThatThrownBy(() -> assertLogicallyEqual(withNull, expected, rows)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> assertLogicallyEqual(expected, withNull, rows)).isInstanceOf(AssertionError.class);

        assertThatThrownBy(() -> assertLogicallyEqual(expected.subList(0, 1), expected, rows)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> assertLogicallyEqual(expected, expected.subList(0, 1), rows)).isInstanceOf(AssertionError.class);
    }

    @Test
    void testJsonTypeDoesNotTakeShreddedPath(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        SimpleGroup row = new SimpleGroup(SHREDDED_SCHEMA);
        Group v = row.addGroup("v");
        v.append("metadata", metadataBinary());
        Group typedValue = v.addGroup("typed_value");
        typedValue.addGroup("a").append("typed_value", 5L);
        typedValue.addGroup("b").append("typed_value", Binary.fromString("hi"));

        // Pins stock 484 behavior for a JSON-typed read of a shredded variant group
        assertThatThrownBy(() -> writeAndReadColumn(SHREDDED_SCHEMA, tempDir, ImmutableList.of(row), JSON))
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining("GroupColumnIO cannot be cast to class org.apache.parquet.io.PrimitiveColumnIO");
    }

    @Test
    void testMissingTopLevelIsVariantNullAndNullGroupIsSqlNull(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v {
                    required binary metadata;
                    optional binary value;
                    optional int64 typed_value;
                  }
                }
                """);
        List<SimpleGroup> rows = new ArrayList<>();
        rows.add(topLevelRow(schema, 5L));
        // group present, value and typed_value both null
        SimpleGroup presentBothNull = new SimpleGroup(schema);
        presentBothNull.addGroup("v").append("metadata", metadataBinary());
        rows.add(presentBothNull);
        rows.add(topLevelRow(schema, 7L));
        // group itself absent
        rows.add(new SimpleGroup(schema));
        rows.add(topLevelRow(schema, 9L));

        assertThat(writeAndReadRawVariants(schema, tempDir, rows)).containsExactly(
                Variant.ofLong(5),
                Variant.NULL_VALUE,
                Variant.ofLong(7),
                null,
                Variant.ofLong(9));
    }

    @Test
    void testMissingArrayElementIsVariantNull(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value (LIST) {
                      repeated group list {
                        required group element {
                          optional binary value;
                          optional int64 typed_value;
                        }
                      }
                    }
                  }
                }
                """);
        List<SimpleGroup> rows = new ArrayList<>();
        rows.add(arrayRow(schema, 1L, null, 3L));
        rows.add(arrayRow(schema, 4L));
        rows.add(arrayRow(schema, null, 6L));

        assertThat(writeAndReadRawVariants(schema, tempDir, rows)).containsExactly(
                Variant.ofArray(ImmutableList.of(Variant.ofLong(1), Variant.NULL_VALUE, Variant.ofLong(3))),
                Variant.ofArray(ImmutableList.of(Variant.ofLong(4))),
                Variant.ofArray(ImmutableList.of(Variant.NULL_VALUE, Variant.ofLong(6))));
    }

    @Test
    void testMalformedShreddedGroupNamesColumn(@TempDir java.nio.file.Path tempDir)
    {
        MessageType schema = variantSchema(
                """
                optional group typed_value {
                  required group event_ts {
                    optional int64 junk;
                  }
                }
                """);
        assertUnsupportedLayout(() -> readEmptyRow(schema, tempDir), "v.typed_value.event_ts");
        assertThatThrownBy(() -> readEmptyRow(schema, tempDir)).hasMessageContaining("neither value nor typed_value");
    }

    @Test
    void testVariantGroupWithExtraChildFails(@TempDir java.nio.file.Path tempDir)
    {
        // unshredded and shredded variants alike: a child other than metadata, value and typed_value is not part of the layout
        assertUnsupportedLayout(() -> readEmptyRow(variantSchema("optional binary value; optional int64 junk;"), tempDir), "v");
        assertUnsupportedLayout(() -> readEmptyRow(variantSchema("optional int64 typed_value; optional int64 junk;"), tempDir), "v");
    }

    @Test
    void testNestedShreddedGroupWithExtraChildFails(@TempDir java.nio.file.Path tempDir)
    {
        MessageType objectField = variantSchema(
                """
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional int64 typed_value;
                    optional int64 junk;
                  }
                }
                """);
        assertUnsupportedLayout(() -> readEmptyRow(objectField, tempDir), "v.typed_value.a");

        MessageType arrayElement = variantSchema(
                """
                optional group typed_value (LIST) {
                  repeated group list {
                    required group element {
                      optional binary value;
                      optional int64 typed_value;
                      optional int64 junk;
                    }
                  }
                }
                """);
        assertUnsupportedLayout(() -> readEmptyRow(arrayElement, tempDir), "v.typed_value.list.element");
    }

    @Test
    void testNonStandardShreddedListFails(@TempDir java.nio.file.Path tempDir)
    {
        // two-level list: the repeated group is the element itself
        assertUnsupportedLayout(() -> readEmptyRow(variantSchema(
                """
                optional group typed_value (LIST) {
                  repeated group element {
                    optional binary value;
                    optional int64 typed_value;
                  }
                }
                """), tempDir), "v.typed_value");
        // the repeated group holds a differently named element
        assertUnsupportedLayout(() -> readEmptyRow(variantSchema(
                """
                optional group typed_value (LIST) {
                  repeated group list {
                    required group item {
                      optional binary value;
                      optional int64 typed_value;
                    }
                  }
                }
                """), tempDir), "v.typed_value");
        // a repeated primitive
        assertUnsupportedLayout(() -> readEmptyRow(variantSchema(
                """
                optional group typed_value (LIST) {
                  repeated int64 element;
                }
                """), tempDir), "v.typed_value");
    }

    @Test
    void testUnsignedIntegerTypedValueFails(@TempDir java.nio.file.Path tempDir)
    {
        for (String typedValue : List.of(
                "optional int32 typed_value (INTEGER(8,false));",
                "optional int32 typed_value (INTEGER(16,false));",
                "optional int32 typed_value (INTEGER(32,false));",
                "optional int64 typed_value (INTEGER(64,false));")) {
            assertUnsupportedLayout(() -> readEmptyRow(variantSchema(typedValue), tempDir), "v.typed_value");
        }
    }

    @Test
    void testDecimalAboveMaxPrecisionFails(@TempDir java.nio.file.Path tempDir)
    {
        for (String typedValue : List.of(
                "optional binary typed_value (DECIMAL(40,2));",
                "optional fixed_len_byte_array(20) typed_value (DECIMAL(40,2));")) {
            assertUnsupportedLayout(() -> readEmptyRow(variantSchema(typedValue), tempDir), "v.typed_value");
        }
    }

    @Test
    void testBinaryTypedValueWithOtherAnnotationFails(@TempDir java.nio.file.Path tempDir)
    {
        for (String annotation : List.of("JSON", "BSON", "ENUM")) {
            assertUnsupportedLayout(() -> readEmptyRow(variantSchema("optional binary typed_value (%s);".formatted(annotation)), tempDir), "v.typed_value");
        }
    }

    @Test
    void testReaderRejectsUnsupportedScalarBlockType()
    {
        // The reader never sees a type the schema conversion rejects, but a type it cannot convert still fails with the column named
        RowType nodeType = RowType.from(ImmutableList.of(RowType.field("typed_value", CharType.createCharType(3))));
        BlockBuilder builder = CharType.createCharType(3).createBlockBuilder(null, 1);
        CharType.createCharType(3).writeSlice(builder, utf8Slice("abc"));
        assertUnsupportedLayout(() -> ShreddedVariantReader.walkNode("v", nodeType, ImmutableList.of(builder.build()), 0), "v.typed_value");
    }

    @Test
    void testCorruptBytesMergedIntoShreddedObjectFailWithColumnName(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = variantSchema(
                """
                optional binary value;
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional int64 typed_value;
                  }
                }
                """);

        // value has an object header but is truncated, so it cannot be merged with the shredded fields
        SimpleGroup garbageValue = new SimpleGroup(schema);
        Group v0 = garbageValue.addGroup("v");
        v0.append("metadata", metadataBinary());
        v0.append("value", Binary.fromConstantByteArray(new byte[] {0x02, 0x05}));
        v0.addGroup("typed_value").addGroup("a").append("typed_value", 1L);
        assertUnsupportedLayout(() -> writeAndReadRawVariants(schema, Files.createTempDirectory(tempDir, "value"), ImmutableList.of(garbageValue)), "v");
        assertThatThrownBy(() -> writeAndReadRawVariants(schema, Files.createTempDirectory(tempDir, "value-cause"), ImmutableList.of(garbageValue)))
                .cause().isNotNull();

        SimpleGroup garbageMetadata = new SimpleGroup(schema);
        Group v1 = garbageMetadata.addGroup("v");
        v1.append("metadata", Binary.fromConstantByteArray(new byte[] {(byte) 0xFF, (byte) 0xFF}));
        v1.addGroup("typed_value").addGroup("a").append("typed_value", 1L);
        assertUnsupportedLayout(() -> writeAndReadRawVariants(schema, Files.createTempDirectory(tempDir, "metadata"), ImmutableList.of(garbageMetadata)), "v");
        assertThatThrownBy(() -> writeAndReadRawVariants(schema, Files.createTempDirectory(tempDir, "metadata-cause"), ImmutableList.of(garbageMetadata)))
                .cause().isNotNull();
    }

    @Test
    void testReadTimeErrorNamesTheFile(@TempDir java.nio.file.Path tempDir)
    {
        MessageType schema = variantSchema(
                """
                optional binary value;
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional int64 typed_value;
                  }
                }
                """);
        SimpleGroup garbageMetadata = new SimpleGroup(schema);
        Group v = garbageMetadata.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(new byte[] {(byte) 0xFF, (byte) 0xFF}));
        v.addGroup("typed_value").addGroup("a").append("typed_value", 1L);
        // an error raised while reading the rows names the reader's data source, here a random id; the cause is kept
        assertThatThrownBy(() -> writeAndReadRawVariants(schema, tempDir, ImmutableList.of(garbageMetadata)))
                .isInstanceOfSatisfying(ShreddedLayoutException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception.getColumnPath()).isEqualTo("v");
                    assertThat(exception.getFile()).isPresent();
                    assertThat(exception.getCause()).isNotNull();
                })
                .hasMessageStartingWith("Unsupported shredded VARIANT layout in column v: invalid variant encoding in the file: ")
                .hasMessageMatching("(?s).* \\(file [0-9a-f-]{36}\\)");

        // a layout refused while the reader's fields are built from the schema cannot name the file: the connector adds it
        assertThatThrownBy(() -> readEmptyRow(variantSchema("optional binary value; optional int64 junk;"), tempDir))
                .isInstanceOfSatisfying(ShreddedLayoutException.class, exception -> assertThat(exception.getFile()).isEmpty())
                .hasMessage("Unsupported shredded VARIANT layout in column v: unexpected child 'junk' beside value and typed_value");
    }

    @Test
    void testInFileKeepsTheErrorAndNamesTheFileOnce()
    {
        Exception cause = new IllegalStateException("cause");
        ShreddedLayoutException exception = new ShreddedLayoutException("v.typed_value", "detail", cause);
        exception.addSuppressed(new RuntimeException("suppressed"));
        ShreddedLayoutException inFile = exception.inFile("s3://bucket/a.parquet");
        assertThat(inFile)
                .hasMessage("Unsupported shredded VARIANT layout in column v.typed_value: detail (file s3://bucket/a.parquet)")
                .hasCause(cause);
        assertThat(inFile.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
        assertThat(inFile.getStackTrace()).isEqualTo(exception.getStackTrace());
        assertThat(inFile.getSuppressed()).isEqualTo(exception.getSuppressed());
        assertThat(inFile.inFile("other.parquet")).isSameAs(inFile);
        assertThat(exception).hasMessage("Unsupported shredded VARIANT layout in column v.typed_value: detail");
    }

    @Test
    void testInconsistentOffsetsFailWithColumnName(@TempDir java.nio.file.Path tempDir)
    {
        MessageType schema = variantSchema(
                """
                optional binary value;
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional int64 typed_value;
                  }
                }
                """);
        Variant dictionary = Variant.ofObject(ImmutableMap.of(utf8Slice("k"), Variant.ofLong(1), utf8Slice("z"), Variant.ofLong(1)));

        // value is an object {k: {k: 1}} whose slot is a byte longer than the object in it: the remapper's own consistency check fires
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(dictionary.metadata().toSlice().getBytes()));
        v.append("value", Binary.fromConstantByteArray(new byte[] {0x02, 0x01, 0x00, 0x00, 0x08, 0x02, 0x01, 0x00, 0x00, 0x02, 0x0C, 0x01, 0x00}));
        v.addGroup("typed_value").addGroup("a").append("typed_value", 1L);

        assertThatThrownBy(() -> writeAndReadRawVariants(schema, tempDir, ImmutableList.of(row)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Unsupported shredded VARIANT layout in column v:")
                .cause()
                .satisfies(cause -> assertThat(cause.getClass().getName()).isEqualTo(ShreddedVariantReader.SPI_VERIFY_EXCEPTION));
    }

    @Test
    void testUnannotatedGroupWithObjectFieldsIsRejected(@TempDir java.nio.file.Path tempDir)
    {
        // no VARIANT annotation, so the footer lowercases eventType and its case cannot be recovered
        MessageType object = unannotatedVariantSchema(
                """
                optional binary value;
                optional group typed_value {
                  required group eventType { optional binary value; optional int64 typed_value; }
                }
                """);
        assertUnsupportedLayout(() -> readEmptyRow(object, tempDir), "v");
        assertThatThrownBy(() -> readEmptyRow(object, tempDir)).hasMessageContaining("lacks the VARIANT logical type annotation");

        // also when the object is inside an array
        MessageType arrayOfObjects = unannotatedVariantSchema(
                """
                optional binary value;
                optional group typed_value (LIST) {
                  repeated group list {
                    required group element {
                      optional binary value;
                      optional group typed_value {
                        required group eventType { optional binary value; optional int64 typed_value; }
                      }
                    }
                  }
                }
                """);
        assertUnsupportedLayout(() -> readEmptyRow(arrayOfObjects, tempDir), "v");
        assertThatThrownBy(() -> readEmptyRow(arrayOfObjects, tempDir)).hasMessageContaining("lacks the VARIANT logical type annotation");
    }

    @Test
    void testUnannotatedGroupWithoutObjectFieldsStillReads(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType primitive = unannotatedVariantSchema("optional binary value;\noptional int64 typed_value;");
        assertThat(writeAndReadRawVariants(primitive, Files.createTempDirectory(tempDir, "primitive"), ImmutableList.of(topLevelRow(primitive, 5L))))
                .containsExactly(Variant.ofLong(5));

        MessageType array = unannotatedVariantSchema(
                """
                optional binary value;
                optional group typed_value (LIST) {
                  repeated group list {
                    required group element {
                      optional binary value;
                      optional int64 typed_value;
                    }
                  }
                }
                """);
        assertThat(writeAndReadRawVariants(array, Files.createTempDirectory(tempDir, "array"), ImmutableList.of(arrayRow(array, 1L, 2L))))
                .containsExactly(Variant.ofArray(ImmutableList.of(Variant.ofLong(1), Variant.ofLong(2))));
    }

    @Test
    void testAnnotatedGroupKeepsFieldNameCase(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group eventType { optional binary value; optional int64 typed_value; }
                    }
                  }
                }
                """);
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", metadataBinary());
        v.addGroup("typed_value").addGroup("eventType").append("typed_value", 5L);
        assertThat(writeAndReadRawVariants(schema, tempDir, ImmutableList.of(row)))
                .containsExactly(Variant.ofObject(ImmutableMap.of(utf8Slice("eventType"), Variant.ofLong(5))));
    }

    @Test
    void testSpiVerifyExceptionClassNameIsPinned()
            throws ClassNotFoundException
    {
        // decodeBytes matches the SPI's package-private VerifyException by name; a rename must fail here
        assertThat(Class.forName(ShreddedVariantReader.SPI_VERIFY_EXCEPTION)).isNotNull();
    }

    private static MessageType unannotatedVariantSchema(String variantChildren)
    {
        return MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v {
                    required binary metadata;
                    %s
                  }
                }
                """.formatted(variantChildren));
    }

    private static MessageType variantSchema(String variantChildren)
    {
        return MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    %s
                  }
                }
                """.formatted(variantChildren));
    }

    // A single row whose variant is absent: the layout problems are found from the schema alone
    private static void readEmptyRow(MessageType schema, java.nio.file.Path tempDir)
    {
        try {
            writeAndReadRawVariants(schema, Files.createTempDirectory(tempDir, "row"), ImmutableList.of(new SimpleGroup(schema)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static SimpleGroup topLevelRow(MessageType schema, long value)
    {
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", metadataBinary());
        v.append("typed_value", value);
        return row;
    }

    private static SimpleGroup arrayRow(MessageType schema, Long... elements)
    {
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", metadataBinary());
        Group typedValue = v.addGroup("typed_value");
        for (Long element : elements) {
            Group group = typedValue.addGroup("list").addGroup("element");
            if (element != null) {
                group.append("typed_value", element);
            }
        }
        return row;
    }

    @Test
    void testObjectFieldMissingVersusVariantNull(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        // field b: both null is absent, value 00 (Variant null) is present with a null value
        SimpleGroup absent = new SimpleGroup(SHREDDED_SCHEMA);
        Group v0 = absent.addGroup("v");
        v0.append("metadata", metadataBinary());
        Group typedValue0 = v0.addGroup("typed_value");
        typedValue0.addGroup("a").append("typed_value", 1L);
        typedValue0.addGroup("b");

        SimpleGroup present = new SimpleGroup(SHREDDED_SCHEMA);
        Group v1 = present.addGroup("v");
        v1.append("metadata", metadataBinary());
        Group typedValue1 = v1.addGroup("typed_value");
        typedValue1.addGroup("a").append("typed_value", 1L);
        typedValue1.addGroup("b").append("value", Binary.fromConstantByteArray(Variant.NULL_VALUE.data().getBytes()));

        assertThat(writeAndReadRawVariants(SHREDDED_SCHEMA, tempDir, ImmutableList.of(absent, present))).containsExactly(
                Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofLong(1))),
                Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofLong(1), utf8Slice("b"), Variant.NULL_VALUE)));
    }

    private static List<Object> writeAndReadVariants(MessageType schema, java.nio.file.Path tempDir, List<SimpleGroup> rows)
            throws IOException
    {
        return writeAndReadColumn(schema, tempDir, rows, VARIANT);
    }

    // Returns the Variant at each position, or null for SQL NULL, so a Variant null stays distinguishable from SQL NULL
    private static List<Variant> writeAndReadRawVariants(MessageType schema, java.nio.file.Path tempDir, List<SimpleGroup> rows)
            throws IOException
    {
        List<Variant> variants = new ArrayList<>();
        writeAndReadColumn(schema, tempDir, rows, VARIANT, (block, position) -> {
            variants.add(block.isNull(position) ? null : (Variant) VARIANT.getObject(block, position));
            return null;
        });
        return variants;
    }

    private static List<Object> writeAndReadColumn(MessageType schema, java.nio.file.Path tempDir, List<SimpleGroup> rows, Type columnType)
            throws IOException
    {
        return writeAndReadColumn(schema, tempDir, rows, columnType, (block, position) -> block.isNull(position) ? null : variantAt(block, position));
    }

    private static List<Object> writeAndReadColumn(MessageType schema, java.nio.file.Path tempDir, List<SimpleGroup> rows, Type columnType, BiFunction<Block, Integer, Object> valueReader)
            throws IOException
    {
        java.nio.file.Path file = tempDir.resolve("shredded.parquet");
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toUri()))
                .withType(schema)
                .withConf(new Configuration())
                .build()) {
            for (SimpleGroup row : rows) {
                writer.write(row);
            }
        }

        List<Type> types = ImmutableList.of(columnType);
        List<String> columnNames = ImmutableList.of("v");
        ParquetDataSource dataSource = new TestingParquetDataSource(
                Slices.wrappedBuffer(Files.readAllBytes(file)),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        // ParquetReader uses an adaptive batch size that starts small, so rows may span pages
        List<Object> variants = new ArrayList<>();
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                Block block = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    variants.add(valueReader.apply(block, position));
                }
            }
        }
        return variants;
    }

    private static Object variantAt(Block block, int position)
    {
        return ((Variant) VARIANT.getObject(block, position)).toObject();
    }

    private static Binary metadataBinary()
    {
        return Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes());
    }
}
