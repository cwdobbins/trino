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

import io.trino.spi.variant.Variant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.variant.VariantBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertUnsupportedLayout;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertVariantsSameAsUnshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.shredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.typedValueCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.unshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeAndReadVariants;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

final class TestShreddedVariantTypes
{
    private static final UUID TEST_UUID = UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c45");

    private record TypeCase(
            String name,
            String typedValueSchema,
            Consumer<VariantBuilder> matching,
            Consumer<VariantBuilder> other,
            Variant expected)
    {
        @Override
        public String toString()
        {
            return name;
        }
    }

    private static Stream<TypeCase> primitiveCases()
    {
        Consumer<VariantBuilder> otherString = builder -> builder.appendString("other");
        Consumer<VariantBuilder> otherInt = builder -> builder.appendInt(7);
        // UUID has no case here: parquet-java's writer never writes a typed UUID (see testUuidMatchesUnshredded)
        return Stream.of(
                new TypeCase("boolean", "optional boolean typed_value;", builder -> builder.appendBoolean(true), otherString, Variant.ofBoolean(true)),
                new TypeCase("int8", "optional int32 typed_value (INT_8);", builder -> builder.appendByte((byte) -5), otherString, Variant.ofByte((byte) -5)),
                new TypeCase("int16", "optional int32 typed_value (INT_16);", builder -> builder.appendShort((short) -300), otherString, Variant.ofShort((short) -300)),
                new TypeCase("int32", "optional int32 typed_value;", builder -> builder.appendInt(-100_000), otherString, Variant.ofInt(-100_000)),
                new TypeCase("int64", "optional int64 typed_value;", builder -> builder.appendLong(-5_000_000_000L), otherString, Variant.ofLong(-5_000_000_000L)),
                new TypeCase("float", "optional float typed_value;", builder -> builder.appendFloat(1.5f), otherString, Variant.ofFloat(1.5f)),
                new TypeCase("double", "optional double typed_value;", builder -> builder.appendDouble(-2.25), otherString, Variant.ofDouble(-2.25)),
                new TypeCase("decimal4", "optional int32 typed_value (DECIMAL(9,2));", builder -> builder.appendDecimal(new BigDecimal("-1234.56")), otherString, Variant.ofDecimal(new BigDecimal("-1234.56"))),
                new TypeCase("decimal8", "optional int64 typed_value (DECIMAL(18,4));", builder -> builder.appendDecimal(new BigDecimal("-12345678901234.5678")), otherString, Variant.ofDecimal(new BigDecimal("-12345678901234.5678"))),
                // parquet-java writes a decimal16 only into a BINARY column
                new TypeCase("decimal16", "optional binary typed_value (DECIMAL(38,6));", builder -> builder.appendDecimal(new BigDecimal("-12345678901234567890123456.123456")), otherString, Variant.ofDecimal(new BigDecimal("-12345678901234567890123456.123456"))),
                new TypeCase("date", "optional int32 typed_value (DATE);", builder -> builder.appendDate(-1000), otherString, Variant.ofDate(-1000)),
                // 12:34:56.789123
                new TypeCase("time", "optional int64 typed_value (TIME(MICROS,false));", builder -> builder.appendTime(45_296_789_123L), otherString, Variant.ofTimeMicrosNtz(45_296_789_123L)),
                // 1969-12-31T23:39:25.432109 UTC, before the epoch with sub-millisecond digits
                new TypeCase("timestamp-micros-utc", "optional int64 typed_value (TIMESTAMP(MICROS,true));", builder -> builder.appendTimestampTz(-1_234_567_891L), otherString, Variant.ofTimestampMicrosUtc(-1_234_567_891L)),
                new TypeCase("timestamp-nanos-utc", "optional int64 typed_value (TIMESTAMP(NANOS,true));", builder -> builder.appendTimestampNanosTz(-1_234_567_891_123L), otherString, Variant.ofTimestampNanosUtc(-1_234_567_891_123L)),
                new TypeCase("timestamp-micros-ntz", "optional int64 typed_value (TIMESTAMP(MICROS,false));", builder -> builder.appendTimestampNtz(-1_234_567_891L), otherString, Variant.ofTimestampMicrosNtz(-1_234_567_891L)),
                new TypeCase("timestamp-nanos-ntz", "optional int64 typed_value (TIMESTAMP(NANOS,false));", builder -> builder.appendTimestampNanosNtz(-1_234_567_891_123L), otherString, Variant.ofTimestampNanosNtz(-1_234_567_891_123L)),
                new TypeCase("binary", "optional binary typed_value;", builder -> builder.appendBinary(ByteBuffer.wrap(new byte[] {1, 2, 3, (byte) 0xFF, 0})), otherString, Variant.ofBinary(wrappedBuffer(new byte[] {1, 2, 3, (byte) 0xFF, 0}))),
                new TypeCase("string", "optional binary typed_value (STRING);", builder -> builder.appendString("héllo"), otherInt, Variant.ofString(utf8Slice("héllo"))));
    }

    @ParameterizedTest
    @MethodSource("primitiveCases")
    void testPrimitiveMatchesUnshredded(TypeCase testCase)
    {
        List<org.apache.parquet.variant.Variant> rows = Arrays.asList(
                variant(testCase.matching()),
                variant(testCase.other()),
                variant(VariantBuilder::appendNull));

        List<Variant> actual = assertVariantsSameAsUnshredded(shredded(testCase.typedValueSchema()), rows, 1);

        assertThat(actual.get(0)).isEqualTo(testCase.expected());
        assertThat(actual.get(2).isNull()).isTrue();
    }

    @Test
    void testByteArrayDecimalIsDecimal(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = shredded("optional binary typed_value (DECIMAL(30,4));");
        BigDecimal value = new BigDecimal("12345678901234567890.1234");
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes()));
        v.append("typed_value", Binary.fromConstantByteArray(value.unscaledValue().toByteArray()));

        java.nio.file.Path file = writeRows(tempDir, "binary-decimal.parquet", schema, row);

        List<Variant> variants = readFile(file.toFile());
        assertThat(variants).hasSize(1);
        assertThat(variants.get(0)).isEqualTo(Variant.ofDecimal(new BigDecimal(new BigInteger("123456789012345678901234"), 4)));
        assertThat(variants.get(0)).isEqualTo(Variant.ofDecimal(value));
    }

    @Test
    void testUnsupportedTypedValueTypeFails(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = shredded("optional int96 typed_value;");
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes()));
        v.append("typed_value", Binary.fromConstantByteArray(new byte[12]));

        java.nio.file.Path file = writeRows(tempDir, "int96.parquet", schema, row);

        assertUnsupportedLayout(() -> readFile(file.toFile()), "v.typed_value");
    }

    @Test
    void testFixedLengthDecimalIsDecimal(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        MessageType schema = shredded("optional fixed_len_byte_array(16) typed_value (DECIMAL(38,6));");
        BigDecimal negative = new BigDecimal("-12345678901234567890123456.123456");
        BigDecimal positive = new BigDecimal("98765432109876543210987654.654321");
        List<SimpleGroup> rows = List.of(
                shreddedRow(schema, "typed_value", Binary.fromConstantByteArray(sixteenBytes(negative.unscaledValue()))),
                shreddedRow(schema, "typed_value", Binary.fromConstantByteArray(sixteenBytes(positive.unscaledValue()))));

        List<Variant> variants = readFile(writeRows(tempDir, "flba-decimal.parquet", schema, rows.toArray(new SimpleGroup[0])).toFile());

        assertThat(variants).containsExactly(Variant.ofDecimal(negative), Variant.ofDecimal(positive));
    }

    @Test
    void testUuidMatchesUnshredded(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        // parquet-java's VariantValueWriter never writes a typed UUID, so the file is built by hand
        MessageType schema = shredded("optional fixed_len_byte_array(16) typed_value (UUID);");
        ByteBuffer uuidBytes = ByteBuffer.allocate(16).putLong(TEST_UUID.getMostSignificantBits()).putLong(TEST_UUID.getLeastSignificantBits());
        org.apache.parquet.variant.Variant other = variant(builder -> builder.appendString("other"));
        org.apache.parquet.variant.Variant variantNull = variant(VariantBuilder::appendNull);
        List<SimpleGroup> rows = List.of(
                shreddedRow(schema, "typed_value", Binary.fromConstantByteArray(uuidBytes.array())),
                shreddedRow(schema, "value", Binary.fromConstantByteArray(toBytes(other.getValueBuffer()))),
                shreddedRow(schema, "value", Binary.fromConstantByteArray(toBytes(variantNull.getValueBuffer()))));

        java.nio.file.Path file = writeRows(tempDir, "uuid.parquet", schema, rows.toArray(new SimpleGroup[0]));
        List<Variant> actual = readFile(file.toFile());

        assertThat(typedValueCount(file.toFile())).isEqualTo(1);
        List<org.apache.parquet.variant.Variant> expectedRows = Arrays.asList(variant(builder -> builder.appendUUID(TEST_UUID)), other, variantNull);
        List<Variant> expected = readFile(writeVariantFile(tempDir, "plain.parquet", unshredded(), expectedRows));
        assertLogicallyEqual(actual, expected, List.of("uuid", "other", "null"));
        assertThat(actual.get(0)).isEqualTo(Variant.ofUuid(TEST_UUID));
    }

    private static SimpleGroup shreddedRow(MessageType schema, String field, Binary value)
    {
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes()));
        v.append(field, value);
        return row;
    }

    private static java.nio.file.Path writeRows(java.nio.file.Path directory, String name, MessageType schema, SimpleGroup... rows)
            throws IOException
    {
        java.nio.file.Path file = directory.resolve(name);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toUri()))
                .withType(schema)
                .withConf(new Configuration())
                .build()) {
            for (SimpleGroup row : rows) {
                writer.write(row);
            }
        }
        return file;
    }

    private static byte[] sixteenBytes(BigInteger value)
    {
        byte[] bytes = value.toByteArray();
        byte[] result = new byte[16];
        Arrays.fill(result, value.signum() < 0 ? (byte) 0xFF : 0);
        System.arraycopy(bytes, 0, result, 16 - bytes.length, bytes.length);
        return result;
    }

    private static byte[] toBytes(ByteBuffer buffer)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    @Test
    void testMillisAndZonedTimeFail()
    {
        for (String typedValue : List.of(
                "optional int64 typed_value (TIME(MICROS,true));",
                "optional int64 typed_value (TIME(NANOS,false));",
                "optional int64 typed_value (TIMESTAMP(MILLIS,false));",
                "optional int64 typed_value (TIMESTAMP(MILLIS,true));",
                "optional int32 typed_value (TIME(MILLIS,false));")) {
            assertUnsupportedLayout(() -> writeAndReadVariants(shredded(typedValue), Arrays.asList(variant(builder -> builder.appendString("x")))), "v.typed_value");
        }
    }

    private static org.apache.parquet.variant.Variant variant(Consumer<VariantBuilder> value)
    {
        VariantBuilder builder = new VariantBuilder();
        value.accept(builder);
        return builder.build();
    }
}
