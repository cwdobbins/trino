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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariantPath
{
    private static final VariantPath ROOT = VariantPath.of();

    @Test
    void testNormalizeDropsCoveredPaths()
    {
        VariantPath a = VariantPath.keys("a");
        VariantPath xy = VariantPath.of(key("x"), element(), key("y"));
        Set<VariantPath> normalized = VariantPath.normalize(List.of(
                VariantPath.keys("a", "b"),
                a,
                VariantPath.keys("a", "b", "c"),
                xy,
                VariantPath.of(key("x"), element(), key("y"))));
        assertThat(normalized).containsExactlyInAnyOrder(a, xy);
    }

    @Test
    void testNormalizeKeyAndElementAtSamePositionBothSurvive()
    {
        VariantPath ab = VariantPath.keys("a", "b");
        VariantPath aElement = VariantPath.of(key("a"), element());
        // neither is a prefix of the other, and the result does not depend on the input order
        assertThat(VariantPath.normalize(List.of(ab, aElement))).containsExactly(ab, aElement);
        assertThat(VariantPath.normalize(List.of(aElement, ab))).containsExactly(ab, aElement);
    }

    @Test
    void testRootCoversEverything()
    {
        assertThat(VariantPath.normalize(List.of(ROOT, VariantPath.keys("a")))).containsExactly(ROOT);
    }

    @Test
    void testKeysAreExact()
    {
        assertThat(key("a")).isNotEqualTo(key("A"));
        assertThat(key("")).isNotEqualTo(element());
        assertThat(key("").isElement()).isFalse();
        assertThat(element().isElement()).isTrue();
    }

    @Test
    void testPrefix()
    {
        VariantPath a = VariantPath.keys("a");
        VariantPath ab = VariantPath.keys("a", "b");
        assertThat(a.isPrefixOf(ab)).isTrue();
        assertThat(a.isPrefixOf(a)).isTrue();
        assertThat(ab.isPrefixOf(a)).isFalse();
        assertThat(VariantPath.of(key("a"), element()).isPrefixOf(ab)).isFalse();
        assertThat(ROOT.isPrefixOf(ab)).isTrue();
    }

    @Test
    void testToStringEscapes()
    {
        assertThat(VariantPath.keys("it's", "a.b").toString()).isEqualTo("$['it\\'s']['a.b']");
        assertThat(VariantPath.of(key("a"), element(), key("b")).toString()).isEqualTo("$['a'][*]['b']");
        assertThat(VariantPath.keys("a\\b").toString()).isEqualTo("$['a\\\\b']");
        assertThat(ROOT.toString()).isEqualTo("$");
    }

    @Test
    void testStepsAreImmutable()
    {
        List<VariantPath.Step> steps = new ArrayList<>(ImmutableList.of(key("a")));
        VariantPath path = new VariantPath(steps);
        steps.add(key("b"));
        assertThat(path.steps()).containsExactly(key("a"));
    }

    @Test
    void testReservedNamesAreOrdinaryKeys()
    {
        VariantPath path = VariantPath.keys("value", "typed_value", "metadata");
        assertThat(path.steps()).containsExactly(key("value"), key("typed_value"), key("metadata"));
        assertThat(path.toString()).isEqualTo("$['value']['typed_value']['metadata']");
    }
}
