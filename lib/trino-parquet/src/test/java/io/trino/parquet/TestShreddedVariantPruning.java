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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestShreddedVariantPruning
{
    private static final String SCHEMA =
            """
            optional group v (VARIANT(1)) {
              required binary metadata;
              optional binary value;
              optional group typed_value {
                required group status { optional binary value; optional binary typed_value (STRING); }
                required group n      { optional binary value; optional int64 typed_value; }
                required group user   { optional binary value; optional group typed_value {
                    required group tier { optional binary value; optional binary typed_value (STRING); }
                    required group geo  { optional binary value; optional group typed_value {
                        required group country { optional binary value; optional binary typed_value (STRING); }
                        required group city    { optional binary value; optional binary typed_value (STRING); } } } } }
                required group items  { optional binary value; optional group typed_value (LIST) { repeated group list {
                    required group element { optional binary value; optional group typed_value {
                        required group sku { optional binary value; optional binary typed_value (STRING); }
                        required group qty { optional binary value; optional int32 typed_value (INTEGER(8,true)); } } } } } }
                required group tags   { optional binary value; optional group typed_value (LIST) { repeated group list {
                    required group element { optional binary value; optional binary typed_value (STRING); } } } }
              }
            }
            """;

    private static GroupType parse(String variantGroup)
    {
        MessageType message = MessageTypeParser.parseMessageType("message m { " + variantGroup + " }");
        return message.getType(0).asGroupType();
    }

    private static void assertPruned(GroupType input, List<VariantPath> paths, String expected)
    {
        assertThat(ShreddedVariantPruning.prune("v", input, paths).toString())
                .isEqualTo(parse(expected).toString());
    }

    private static void assertPruned(List<VariantPath> paths, String expected)
    {
        assertPruned(parse(SCHEMA), paths, expected);
    }

    @Test
    void testScalarPath()
    {
        // status
        assertPruned(
                ImmutableList.of(VariantPath.keys("status")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group status { optional binary value; optional binary typed_value (STRING); } } }");
    }

    @Test
    void testNestedPath()
    {
        // user.geo.country
        assertPruned(
                ImmutableList.of(VariantPath.keys("user", "geo", "country")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group user { optional binary value; optional group typed_value { required group geo { optional binary value; optional group typed_value { required group country { optional binary value; optional binary typed_value (STRING); } } } } } } }");
    }

    @Test
    void testListMemberPath()
    {
        // items[*].sku
        assertPruned(
                ImmutableList.of(VariantPath.of(key("items"), element(), key("sku"))),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group items { optional binary value; optional group typed_value (LIST) { repeated group list { required group element { optional binary value; optional group typed_value { required group sku { optional binary value; optional binary typed_value (STRING); } } } } } } } }");
    }

    @Test
    void testListOfScalars()
    {
        // tags[*]
        assertPruned(
                ImmutableList.of(VariantPath.of(key("tags"), element())),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group tags { optional binary value; optional group typed_value (LIST) { repeated group list { required group element { optional binary value; optional binary typed_value (STRING); } } } } } }");
    }

    @Test
    void testPathEndingAtObject()
    {
        // user
        assertPruned(
                ImmutableList.of(VariantPath.keys("user")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group user { optional binary value; optional group typed_value { required group tier { optional binary value; optional binary typed_value (STRING); } required group geo { optional binary value; optional group typed_value { required group country { optional binary value; optional binary typed_value (STRING); } required group city { optional binary value; optional binary typed_value (STRING); } } } } } } }");
    }

    @Test
    void testKeyNotShredded()
    {
        // zzz
        assertPruned(
                ImmutableList.of(VariantPath.keys("zzz")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group status { optional binary typed_value (STRING); } } }");
    }

    @Test
    void testNestedKeyNotShredded()
    {
        // user.zzz
        assertPruned(
                ImmutableList.of(VariantPath.keys("user", "zzz")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group user { optional binary value; optional group typed_value { required group tier { optional binary typed_value (STRING); } } } } }");
    }

    @Test
    void testKeyStepOnList()
    {
        // items.sku
        assertPruned(
                ImmutableList.of(VariantPath.keys("items", "sku")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group items { optional binary value; optional group typed_value (LIST) { repeated group list { required group element { optional binary value; optional group typed_value { required group sku { optional binary value; optional binary typed_value (STRING); } required group qty { optional binary value; optional int32 typed_value (INTEGER(8,true)); } } } } } } } }");
    }

    @Test
    void testElementStepOnObject()
    {
        // user[*]
        assertPruned(
                ImmutableList.of(VariantPath.of(key("user"), element())),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group user { optional binary value; optional group typed_value { required group tier { optional binary value; optional binary typed_value (STRING); } required group geo { optional binary value; optional group typed_value { required group country { optional binary value; optional binary typed_value (STRING); } required group city { optional binary value; optional binary typed_value (STRING); } } } } } } }");
    }

    @Test
    void testKeyStepOnPrimitive()
    {
        // status.x
        assertPruned(
                ImmutableList.of(VariantPath.keys("status", "x")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group status { optional binary value; optional binary typed_value (STRING); } } }");
    }

    @Test
    void testUnionOfPaths()
    {
        // status, items[*].qty, user.tier
        assertPruned(
                ImmutableList.of(VariantPath.keys("status"), VariantPath.of(key("items"), element(), key("qty")), VariantPath.keys("user", "tier")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group status { optional binary value; optional binary typed_value (STRING); } required group user { optional binary value; optional group typed_value { required group tier { optional binary value; optional binary typed_value (STRING); } } } required group items { optional binary value; optional group typed_value (LIST) { repeated group list { required group element { optional binary value; optional group typed_value { required group qty { optional binary value; optional int32 typed_value (INTEGER(8,true)); } } } } } } } }");
    }

    @Test
    void testWitnessSupersededByRealPath()
    {
        // zzz, n
        assertPruned(
                ImmutableList.of(VariantPath.keys("zzz"), VariantPath.keys("n")),
                "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group n { optional binary value; optional int64 typed_value; } } }");
    }

    @Test
    void testCoveredPathsCollapse()
    {
        String expected = "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group user { optional binary value; optional group typed_value { required group tier { optional binary value; optional binary typed_value (STRING); } required group geo { optional binary value; optional group typed_value { required group country { optional binary value; optional binary typed_value (STRING); } required group city { optional binary value; optional binary typed_value (STRING); } } } } } } }";
        assertPruned(ImmutableList.of(VariantPath.keys("user"), VariantPath.keys("user", "geo", "country")), expected);
        assertPruned(ImmutableList.of(VariantPath.keys("user")), expected);
    }

    @Test
    void testRootReturnsInput()
    {
        GroupType input = parse(SCHEMA);
        assertThat(ShreddedVariantPruning.prune("v", input, ImmutableList.of(VariantPath.of(), VariantPath.keys("status")))).isEqualTo(input);
    }

    @Test
    void testUnshreddedGroupUnchanged()
    {
        GroupType input = parse("optional group v (VARIANT(1)) { required binary metadata; required binary value; }");
        assertThat(ShreddedVariantPruning.prune("v", input, ImmutableList.of(VariantPath.keys("a", "b")))).isEqualTo(input);
    }

    @Test
    void testUnshreddedGroupWithFixedLengthValueUnchanged()
    {
        // the readers accept a FIXED_LEN_BYTE_ARRAY value in an unshredded group, so pruning must not reject it
        GroupType input = parse("optional group v (VARIANT(1)) { required binary metadata; required fixed_len_byte_array(4) value; }");
        assertThat(ShreddedVariantPruning.prune("v", input, ImmutableList.of(VariantPath.keys("a")))).isEqualTo(input);
    }

    @Test
    void testNodeWithoutTypedValueKeepsOnlyValue()
    {
        // rule 8: the "a" level is not shredded in this file
        assertPruned(
                parse(PLAIN_A),
                ImmutableList.of(VariantPath.keys("a", "x", "y")),
                PLAIN_A_PRUNED);
    }

    private static final String PLAIN_A = "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group a { optional binary value; } required group b { optional binary value; optional binary typed_value (STRING); } } }";
    private static final String PLAIN_A_PRUNED = "optional group v (VARIANT(1)) { required binary metadata; optional binary value; optional group typed_value { required group a { optional binary value; } } }";

    @Test
    void testWitnessDescendsWhenFieldHasNoValue()
    {
        // the first field has neither a primitive typed_value nor a value: the witness descends into it
        assertPruned(
                parse("optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional group typed_value { required group x { optional binary value; optional int64 typed_value; } required group y { optional binary typed_value (STRING); } } } required group b { optional binary typed_value (STRING); } } }"),
                ImmutableList.of(VariantPath.keys("zzz")),
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional group typed_value { required group x { optional int64 typed_value; } } } } }");
    }

    @Test
    void testAnnotationIdAndRepetitionKept()
    {
        GroupType input = Types.buildGroup(REQUIRED)
                .id(7)
                .as(LogicalTypeAnnotation.variantType((byte) 1))
                .addField(Types.required(BINARY).named("metadata"))
                .addField(Types.optional(BINARY).named("value"))
                .addField(Types.buildGroup(OPTIONAL)
                        .addField(field("a"))
                        .addField(Types.buildGroup(REQUIRED)
                                .addField(Types.optional(BINARY).named("value"))
                                .named("b"))
                        .named("typed_value"))
                .named("v");
        GroupType pruned = ShreddedVariantPruning.prune("v", input, ImmutableList.of(VariantPath.keys("b")));
        assertThat(pruned.getName()).isEqualTo("v");
        assertThat(pruned.getRepetition()).isEqualTo(REQUIRED);
        assertThat(pruned.getId()).isEqualTo(input.getId());
        assertThat(pruned.getLogicalTypeAnnotation()).isEqualTo(input.getLogicalTypeAnnotation());
        assertThat(pruned.getType("typed_value").asGroupType().getFieldCount()).isEqualTo(1);
        assertThat(pruned.getType("typed_value").asGroupType().getType("b")).isEqualTo(input.getType("typed_value").asGroupType().getType("b"));
    }

    @Test
    void testReservedAndAwkwardKeys()
    {
        GroupType input = Types.buildGroup(OPTIONAL)
                .as(LogicalTypeAnnotation.variantType((byte) 1))
                .addField(Types.required(BINARY).named("metadata"))
                .addField(Types.optional(BINARY).named("value"))
                .addField(Types.buildGroup(OPTIONAL)
                        .addField(field("value"))
                        .addField(field("typed_value"))
                        .addField(field(""))
                        .addField(field("a.b"))
                        .addField(field("A"))
                        .addField(field("a"))
                        .named("typed_value"))
                .named("v");
        for (String name : ImmutableList.of("value", "typed_value", "", "a.b", "A", "a")) {
            GroupType pruned = ShreddedVariantPruning.prune("v", input, ImmutableList.of(VariantPath.keys(name)));
            GroupType objectGroup = pruned.getType("typed_value").asGroupType();
            assertThat(objectGroup.getFieldCount()).describedAs(name).isEqualTo(1);
            assertThat(objectGroup.getType(0)).describedAs(name).isEqualTo(field(name));
        }
    }

    private static GroupType field(String name)
    {
        return Types.buildGroup(REQUIRED)
                .addField(Types.optional(BINARY).named("value"))
                .addField(Types.optional(BINARY).as(stringType()).named("typed_value"))
                .named(name);
    }

    @Test
    void testWitnessUsesValueBeforeDescending()
    {
        // the first field has a value and an object typed_value: its value is the witness, not a column inside the object
        assertPruned(
                parse("optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional binary value; optional group typed_value { required group x { optional binary typed_value (STRING); } } } required group b { optional binary typed_value (STRING); } } }"),
                ImmutableList.of(VariantPath.keys("zzz")),
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional binary value; } } }");
    }

    private static void assertRejected(String schema, VariantPath path, String detail)
    {
        GroupType input = parse(schema);
        String message = "Unsupported shredded VARIANT layout in column v" + detail;
        assertThatThrownBy(() -> ParquetTypeUtils.checkShreddedLayout("v", input))
                .isInstanceOf(ShreddedLayoutException.class)
                .hasMessageContaining(message);
        assertThatThrownBy(() -> ShreddedVariantPruning.prune("v", input, ImmutableList.of(path)))
                .isInstanceOf(ShreddedLayoutException.class)
                .hasMessageContaining(message);
    }

    @Test
    void testPrimitiveObjectFieldRejected()
    {
        assertRejected(
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional binary value; } required binary b; } }",
                VariantPath.keys("a"),
                ".typed_value.b: object field 'b' is not a group holding value and typed_value");
    }

    @Test
    void testListWithEmptyRepeatedGroupRejected()
    {
        assertRejected(
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group l { optional binary value; optional group typed_value (LIST) { repeated group list { } } } } }",
                VariantPath.of(key("l"), element()),
                ".typed_value.l.typed_value: array is not a three-level list");
    }

    @Test
    void testListWithSecondRepeatedChildRejected()
    {
        assertRejected(
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group l { optional binary value; optional group typed_value (LIST) { repeated group list { required group element { optional binary value; optional binary typed_value (STRING); } optional binary extra; } } } } }",
                VariantPath.of(key("l"), element()),
                ".typed_value.l.typed_value: array is not a three-level list");
    }

    @Test
    void testWitnessFieldWithUnknownSiblingRejected()
    {
        assertRejected(
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional binary extra; optional binary typed_value (STRING); } } }",
                VariantPath.keys("zzz"),
                ".typed_value.a: unexpected child 'extra' beside value and typed_value");
    }

    @Test
    void testNodeWithNeitherValueNorTypedValueRejected()
    {
        assertRejected(
                "optional group v (VARIANT(1)) { required binary metadata; optional group typed_value { required group a { optional binary value; } required group b { optional binary other; } } }",
                VariantPath.keys("a"),
                ".typed_value.b: group has neither value nor typed_value");
    }

    @Test
    void testEmptyPathSetRejected()
    {
        assertThatThrownBy(() -> ShreddedVariantPruning.prune("v", parse(SCHEMA), ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
