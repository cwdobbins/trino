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
import io.trino.parquet.reader.ShreddedValue.ShreddedArray;
import io.trino.parquet.reader.ShreddedValue.ShreddedObject;
import io.trino.parquet.reader.ShreddedValue.ShreddedScalar;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariantShreddingReconstructor
{
    @Test
    void testScalarFromTypedValue()
    {
        ShreddedValue node = new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(5)));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(5L);
    }

    @Test
    void testScalarFallsBackToValueWhenNotShredded()
    {
        Variant string = Variant.ofString("hi");
        ShreddedValue node = new ShreddedScalar(Optional.of(string.data()), Optional.empty());
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo("hi");
    }

    @Test
    void testMissingScalarReturnsEmpty()
    {
        ShreddedValue node = new ShreddedScalar(Optional.empty(), Optional.empty());
        assertThat(VariantShreddingReconstructor.reconstruct(node, EMPTY_METADATA)).isEmpty();
    }

    @Test
    void testFullyShreddedObject()
    {
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofString("x"))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("a", 1L, "b", "x"));
    }

    @Test
    void testPartiallyShreddedObjectMergesValueFields()
    {
        // value holds the non-shredded field c; the shredded field a comes from typed_value
        Variant partialValue = Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.ofLong(3)));
        ShreddedValue node = new ShreddedObject(
                Optional.of(partialValue.data()),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))))));
        assertThat(reconstruct(node, partialValue.metadata())).isEqualTo(ImmutableMap.of("a", 1L, "c", 3L));
    }

    @Test
    void testObjectOmitsMissingShreddedField()
    {
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.empty()),
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(2))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("b", 2L));
    }

    @Test
    void testMissingArrayElementIsVariantNull()
    {
        ShreddedValue node = new ShreddedArray(
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        new ShreddedScalar(Optional.empty(), Optional.empty()),
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(3))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(Arrays.asList(1L, null, 3L));
    }

    @Test
    void testMissingObjectFieldIsAbsent()
    {
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.empty()))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("a", 1L));
    }

    @Test
    void testNullObjectFieldIsPresent()
    {
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        utf8Slice("b"), new ShreddedScalar(Optional.of(Variant.NULL_VALUE.data()), Optional.empty()))));
        Map<String, Object> expected = new HashMap<>();
        expected.put("a", 1L);
        expected.put("b", null);
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(expected);
    }

    @Test
    void testNestedMissingRules()
    {
        // an object field inside an array element: absent inside the element; an array inside an object field keeps a Variant null element
        ShreddedValue elementObject = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("x"), new ShreddedScalar(Optional.empty(), Optional.empty()),
                        utf8Slice("y"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(2))))));
        ShreddedValue innerArray = new ShreddedArray(
                Optional.empty(),
                Optional.of(ImmutableList.of(new ShreddedScalar(Optional.empty(), Optional.empty()))));
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("arr"), new ShreddedArray(Optional.empty(), Optional.of(ImmutableList.of(elementObject))),
                        utf8Slice("nulls"), innerArray)));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of(
                "arr", ImmutableList.of(ImmutableMap.of("y", 2L)),
                "nulls", Collections.singletonList(null)));
    }

    @Test
    void testNestedObject()
    {
        ShreddedValue inner = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))))));
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(utf8Slice("a"), inner)));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("a", ImmutableMap.of("b", 1L)));
    }

    @Test
    void testShreddedArray()
    {
        ShreddedValue node = new ShreddedArray(
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofString("x"))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableList.of(1L, "x"));
    }

    @Test
    void testArrayFallsBackToValueWhenNotShredded()
    {
        Variant array = Variant.ofArray(ImmutableList.of(Variant.ofLong(1), Variant.ofLong(2)));
        ShreddedValue node = new ShreddedArray(Optional.of(array.data()), Optional.empty());
        assertThat(reconstruct(node, array.metadata())).isEqualTo(ImmutableList.of(1L, 2L));
    }

    @Test
    void testShreddedFieldAlsoInValueUsesTypedValue()
    {
        // INVALID per the spec: value repeats the shredded field event_type; typed_value must be used
        Variant value = Variant.ofObject(ImmutableMap.of(utf8Slice("event_type"), Variant.ofString("login")));
        ShreddedValue node = new ShreddedObject(
                Optional.of(value.data()),
                Optional.of(ImmutableMap.of(
                        utf8Slice("event_type"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofString("login"))),
                        utf8Slice("event_ts"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1729795057774L))))));
        assertThat(reconstruct(node, value.metadata())).isEqualTo(ImmutableMap.of("event_type", "login", "event_ts", 1729795057774L));

        // value's copy differs: typed_value wins
        Variant differing = Variant.ofObject(ImmutableMap.of(utf8Slice("event_type"), Variant.ofString("x")));
        ShreddedValue differingNode = new ShreddedObject(
                Optional.of(differing.data()),
                Optional.of(ImmutableMap.of(
                        utf8Slice("event_type"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofString("login"))),
                        utf8Slice("event_ts"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1729795057774L))))));
        assertThat(reconstruct(differingNode, differing.metadata())).isEqualTo(ImmutableMap.of("event_type", "login", "event_ts", 1729795057774L));
    }

    @Test
    void testNonObjectWithBothSetUsesTypedValue()
    {
        ShreddedValue node = new ShreddedScalar(Optional.of(Variant.ofString("a").data()), Optional.of(Variant.ofLong(34)));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(34L);
    }

    @Test
    void testArrayWithBothSetUsesTypedValue()
    {
        Variant value = Variant.ofArray(ImmutableList.of(Variant.ofLong(9)));
        ShreddedValue node = new ShreddedArray(
                Optional.of(value.data()),
                Optional.of(ImmutableList.of(
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(2))))));
        assertThat(reconstruct(node, value.metadata())).isEqualTo(ImmutableList.of(1L, 2L));
    }

    @Test
    void testObjectTypedPresentAndValueNotAnObjectUsesTypedValue()
    {
        // INVALID per the spec ("typed_value is present and value is not an object"): typed_value is used
        Variant value = Variant.ofString("a");
        ShreddedValue node = new ShreddedObject(
                Optional.of(value.data()),
                Optional.of(ImmutableMap.of(utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))))));
        assertThat(reconstruct(node, value.metadata())).isEqualTo(ImmutableMap.of("a", 1L));
    }

    @Test
    void testObjectTypedNullAndValueObjectReturnsValue()
    {
        Variant empty = Variant.ofObject(ImmutableMap.of());
        ShreddedValue node = new ShreddedObject(Optional.of(empty.data()), Optional.empty());
        assertThat(reconstruct(node, empty.metadata())).isEqualTo(ImmutableMap.of());
    }

    private static Object reconstruct(ShreddedValue node, Metadata metadata)
    {
        return VariantShreddingReconstructor.reconstruct(node, metadata).orElseThrow().toObject();
    }
}
