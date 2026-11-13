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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.VariantPath;
import io.trino.parquet.reader.VariantPathExtraction.Extraction;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.toJson;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.kind;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The oracle itself: values the reader could return that a query would tell apart must compare as different.
 */
final class TestVariantPathExtraction
{
    @Test
    void testIntegerWidthsDiffer()
    {
        // logically equal, the same JSON, a different kind: only the kind check tells them apart
        Variant int8 = Variant.ofByte((byte) 1);
        Variant int16 = Variant.ofShort((short) 1);
        assertThat(int8).isEqualTo(int16);
        assertThat(toJson(int8)).isEqualTo(toJson(int16));
        assertDifferent(int8, int16);
        assertDifferent(int8, Variant.ofInt(1));
        assertDifferent(int8, Variant.ofLong(1));
    }

    @Test
    void testIntegerAndDecimalDiffer()
    {
        // logically equal, different JSON
        Variant int8 = Variant.ofByte((byte) 1);
        Variant decimal = Variant.ofDecimal(new BigDecimal("1.00"));
        assertThat(int8).isEqualTo(decimal);
        assertThat(toJson(int8)).isNotEqualTo(toJson(decimal));
        assertDifferent(int8, decimal);
    }

    @Test
    void testDecimalScalesDiffer()
    {
        Variant one = Variant.ofDecimal(new BigDecimal("1.0"));
        Variant oneHundredths = Variant.ofDecimal(new BigDecimal("1.00"));
        assertThat(kind(one)).isEqualTo(kind(oneHundredths));
        assertThat(one).isEqualTo(oneHundredths);
        assertDifferent(one, oneHundredths);
        assertSame(one, Variant.ofDecimal(new BigDecimal("1.0")));
    }

    @Test
    void testDoubleAndIntegerDiffer()
    {
        Variant dbl = Variant.ofDouble(1.0);
        Variant int8 = Variant.ofByte((byte) 1);
        assertThat(dbl).isEqualTo(int8);
        assertDifferent(dbl, int8);
        assertDifferent(Variant.ofFloat(1.0f), dbl);
    }

    @Test
    void testStringLikeKinds()
    {
        Variant shortString = Variant.ofString("a");
        assertThat(shortString.basicType()).isEqualTo(BasicType.SHORT_STRING);
        assertDifferent(shortString, Variant.ofBinary(utf8Slice("a")));
        assertDifferent(shortString, Variant.ofString("b"));
        // Trino names a short string and a long-form string both "string" and renders them alike: the same value
        Variant longString = longFormString("a");
        assertThat(longString.basicType()).isEqualTo(BasicType.PRIMITIVE);
        assertThat(longString.primitiveType()).isEqualTo(PrimitiveType.STRING);
        assertSame(shortString, longString);
    }

    @Test
    void testNestedDifferences()
    {
        Variant object = object(ImmutableMap.of("o", object(ImmutableMap.of("x", Variant.ofByte((byte) 1)))));
        assertDifferent(object, object(ImmutableMap.of("o", object(ImmutableMap.of("x", Variant.ofByte((byte) 1), "y", Variant.ofByte((byte) 2))))));
        assertDifferent(object, object(ImmutableMap.of("o", object(ImmutableMap.of("x", Variant.ofShort((short) 1))))));
        assertDifferent(object, object(ImmutableMap.of("o", object(ImmutableMap.of("x", Variant.ofDecimal(new BigDecimal("1.0")))))));
        Variant array = Variant.ofArray(ImmutableList.of(Variant.ofByte((byte) 1), Variant.NULL_VALUE));
        assertDifferent(array, Variant.ofArray(ImmutableList.of(Variant.ofShort((short) 1), Variant.NULL_VALUE)));
        assertDifferent(array, Variant.ofArray(ImmutableList.of(Variant.ofByte((byte) 1))));
        assertSame(array, Variant.ofArray(ImmutableList.of(Variant.ofByte((byte) 1), Variant.NULL_VALUE)));
    }

    @Test
    void testDifferenceInsideElements()
    {
        // an element step compares each element's value by the same rule
        Variant left = object(ImmutableMap.of("l", Variant.ofArray(ImmutableList.of(Variant.ofString("a"), Variant.ofByte((byte) 1)))));
        Variant right = object(ImmutableMap.of("l", Variant.ofArray(ImmutableList.of(Variant.ofString("a"), Variant.ofDecimal(new BigDecimal("1.00"))))));
        VariantPath path = VariantPath.of(key("l"), element());
        assertThat(same(extract(left, path), extract(right, path))).isFalse();
        assertThat(same(extract(left, path), extract(left, path))).isTrue();
    }

    @Test
    void testByteIdenticalValuesSame()
    {
        Variant object = object(ImmutableMap.of(
                "s", Variant.ofString("text"),
                "d", Variant.ofDecimal(new BigDecimal("12.50")),
                "l", Variant.ofArray(ImmutableList.of(Variant.ofLong(5), Variant.NULL_VALUE, Variant.ofBoolean(true)))));
        Variant copy = Variant.from(object.metadata(), Slices.wrappedBuffer(object.data().getBytes()));
        assertSame(object, copy);
        assertSame(Variant.NULL_VALUE, Variant.NULL_VALUE);
    }

    @Test
    void testNonValueExtractions()
    {
        assertThat(same(new Extraction.Missing(), new Extraction.Missing())).isTrue();
        assertThat(same(new Extraction.SqlNull(), new Extraction.SqlNull())).isTrue();
        assertThat(same(new Extraction.WrongShape(1, "string"), new Extraction.WrongShape(1, "string"))).isTrue();
        assertThat(same(new Extraction.WrongShape(1, "string"), new Extraction.WrongShape(0, "string"))).isFalse();
        assertThat(same(new Extraction.WrongShape(1, "int8"), new Extraction.WrongShape(1, "int16"))).isFalse();
        assertThat(same(new Extraction.Missing(), new Extraction.Value(Variant.NULL_VALUE))).isFalse();
        assertThat(same(new Extraction.SqlNull(), new Extraction.Value(Variant.NULL_VALUE))).isFalse();
        assertThat(extract(Variant.NULL_VALUE, VariantPath.keys("a"))).isEqualTo(new Extraction.WrongShape(0, "null"));
    }

    private static void assertDifferent(Variant left, Variant right)
    {
        assertThat(same(new Extraction.Value(left), new Extraction.Value(right))).as("%s vs %s", toJson(left), toJson(right)).isFalse();
        assertThat(same(new Extraction.Value(right), new Extraction.Value(left))).as("%s vs %s", toJson(right), toJson(left)).isFalse();
    }

    private static void assertSame(Variant left, Variant right)
    {
        assertThat(same(new Extraction.Value(left), new Extraction.Value(right))).as("%s vs %s", toJson(left), toJson(right)).isTrue();
    }

    private static Variant object(ImmutableMap<String, Variant> fields)
    {
        ImmutableMap.Builder<Slice, Variant> builder = ImmutableMap.builder();
        fields.forEach((name, value) -> builder.put(utf8Slice(name), value));
        return Variant.ofObject(builder.buildOrThrow());
    }

    // A string in the primitive (long) form, which the SPI's builders use only for strings over 63 bytes
    private static Variant longFormString(String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        Slice data = Slices.allocate(1 + 4 + bytes.length);
        data.setByte(0, PrimitiveType.STRING.ordinal() << 2);
        data.setInt(1, bytes.length);
        data.setBytes(5, bytes);
        return Variant.from(EMPTY_METADATA, data);
    }
}
