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
import io.trino.parquet.VariantPath;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergVariantProjection
{
    private static final IcebergColumnHandle DOCUMENT = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "doc")).columnType(VARIANT).build();
    private static final RowType ROW_TYPE = RowType.from(List.of(RowType.field("x", BIGINT)));
    private static final IcebergColumnHandle ROW = IcebergColumnHandle.optional(new ColumnIdentity(
            2,
            "r",
            STRUCT,
            List.of(primitiveColumnIdentity(3, "x")))).columnType(ROW_TYPE).build();
    private static final IcebergTableHandle TABLE = new IcebergTableHandle(
            "s",
            "t",
            TableType.DATA,
            OptionalLong.empty(),
            "{}",
            OptionalInt.empty(),
            Map.of(),
            3,
            TupleDomain.all(),
            TupleDomain.all(),
            OptionalLong.empty(),
            Set.of(),
            Optional.empty(),
            "memory:///",
            Map.of(),
            Optional.empty(),
            false,
            Optional.empty(),
            Set.of(),
            Optional.empty());

    @Test
    void testDistinctVariantDemandsAlongsideRowDereference()
    {
        IcebergColumnHandle first = DOCUMENT.withVariantPaths(Set.of(VariantPath.keys("a")));
        IcebergColumnHandle second = DOCUMENT.withVariantPaths(Set.of(VariantPath.keys("b")));
        assertProjection(first, second);
        assertProjection(DOCUMENT, first);
    }

    private static void assertProjection(IcebergColumnHandle first, IcebergColumnHandle second)
    {
        List<ConnectorExpression> expressions = ImmutableList.of(
                new Variable("first", VARIANT),
                new Variable("second", VARIANT),
                new FieldDereference(BIGINT, new Variable("row", ROW_TYPE), 0));
        Map<String, ColumnHandle> inputs = ImmutableMap.of("first", first, "second", second, "row", ROW);
        var result = IcebergMetadata.projectColumns(TABLE, expressions, inputs).orElseThrow();
        assertThat(result.getAssignments()).hasSize(3);
        assertThat(result.getAssignments()).extracting(Assignment::getVariable).doesNotHaveDuplicates();
        Map<String, ColumnHandle> bindings = result.getAssignments().stream()
                .collect(toImmutableMap(Assignment::getVariable, Assignment::getColumn));
        assertThat(bindings.get(((Variable) result.getProjections().get(0)).getName())).isEqualTo(first);
        assertThat(bindings.get(((Variable) result.getProjections().get(1)).getName())).isEqualTo(second);
        assertThat(((IcebergColumnHandle) bindings.get(((Variable) result.getProjections().get(2)).getName())).getPath()).containsExactly(3);
        assertThat(((IcebergTableHandle) result.getHandle()).getProjectedColumns()).contains(first, second);
        assertThat(first.getName()).isEqualTo("doc");
        assertThat(second.getName()).isEqualTo("doc");
        assertThat(IcebergMetadata.projectColumns((IcebergTableHandle) result.getHandle(), result.getProjections(), bindings)).isEmpty();
        assertThat(IcebergMetadata.projectColumns(TABLE, expressions, inputs).orElseThrow().getProjections()).isEqualTo(result.getProjections());
    }

    @Test
    void testIdenticalAliasesShareAssignment()
    {
        IcebergColumnHandle pruned = DOCUMENT.withVariantPaths(Set.of(VariantPath.keys("a")));
        var result = IcebergMetadata.projectColumns(
                TABLE,
                List.of(
                        new Variable("a", VARIANT),
                        new Variable("b", VARIANT),
                        new FieldDereference(BIGINT, new Variable("row", ROW_TYPE), 0)),
                ImmutableMap.of("a", pruned, "b", pruned, "row", ROW)).orElseThrow();
        assertThat(result.getAssignments()).hasSize(2);
        assertThat(result.getProjections().get(0)).isEqualTo(result.getProjections().get(1));
    }

    @Test
    void testGeneratedNamesDoNotCollideWithPhysicalNames()
    {
        IcebergColumnHandle namedLikeAlias = IcebergColumnHandle.optional(primitiveColumnIdentity(4, "doc$1")).columnType(BIGINT).build();
        var result = IcebergMetadata.projectColumns(
                TABLE,
                List.of(
                        new Variable("a", VARIANT),
                        new Variable("b", VARIANT),
                        new Variable("c", BIGINT),
                        new FieldDereference(BIGINT, new Variable("row", ROW_TYPE), 0)),
                ImmutableMap.of("a", DOCUMENT, "b", DOCUMENT.withVariantPaths(Set.of(VariantPath.keys("b"))), "c", namedLikeAlias, "row", ROW)).orElseThrow();
        assertThat(result.getAssignments()).hasSize(4).extracting(Assignment::getVariable).doesNotHaveDuplicates();
        assertThat(result.getAssignments()).extracting(Assignment::getColumn).contains(namedLikeAlias);
    }
}
