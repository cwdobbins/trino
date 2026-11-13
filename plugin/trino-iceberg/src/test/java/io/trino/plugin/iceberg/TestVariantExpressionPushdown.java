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

import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.variant.Variant;
import io.trino.sql.planner.InternalConnectorExpressionEvaluator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.VariantExpressionTestHarness.filters;
import static io.trino.plugin.iceberg.VariantExpressionTestHarness.functions;
import static io.trino.plugin.iceberg.VariantExpressionTestHarness.functionsWithoutOpaque;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestVariantExpressionPushdown
{
    private VariantExpressionTestHarness harness;

    @BeforeAll
    void setUp()
    {
        harness = new VariantExpressionTestHarness();
    }

    @AfterAll
    void tearDown()
    {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void testNativeExpressionTranslation()
    {
        Map<String, String> cases = Map.of(
                "doc['a']['b'] = CAST('x' AS VARIANT)", "$equal",
                "doc['a'] IN (CAST('x' AS VARIANT), CAST(NULL AS VARIANT))", "$in",
                "doc['a'] IS NULL", "$is_null",
                "doc['a'] IS NOT NULL", "$not",
                "CAST(doc['a'] AS BIGINT) > 5", "$cast",
                "TRY_CAST(doc['a'] AS DOUBLE) > 5.0", "$try_cast",
                "CAST(doc['a'] AS VARCHAR) = 'x'", "$cast",
                "any_match(CAST(doc['items'] AS ARRAY(VARIANT)), e -> e['sku'] = CAST('x' AS VARIANT))", "any_match");
        cases.forEach((sql, name) -> assertThat(functions(harness.expression(sql))).as(sql).contains(name, "$operator$subscript"));
    }

    @Test
    void testOpaqueAndTransparentPasses()
    {
        String predicate = "doc['a'] = CAST(7 AS VARIANT)";
        assertThat(filters(harness.plan(predicate, true, false))).contains("doc");
        assertThat(harness.observed()).anySatisfy(constraint -> assertThat(functions(constraint.getExpression())).contains("$equal").doesNotContain("$engine_expression"));
        assertThat(harness.observed()).anySatisfy(constraint -> assertThat(functions(constraint.getExpression())).contains("$engine_expression"));
    }

    @Test
    void testUntranslatedResidualsRemain()
    {
        for (String residual : List.of(
                "TRY(CAST(doc['a'] AS BIGINT)) = 7",
                "CASE WHEN flag THEN CAST(doc['a'] AS BIGINT) ELSE 0 END = 7",
                "IF(flag, CAST(doc['a'] AS BIGINT), 0) = 7")) {
            String predicate = "phase3_probe_is_object(doc) AND " + residual;
            assertThat(filters(harness.plan(predicate, true, true))).as(residual)
                    .contains("doc").doesNotContain("phase3_probe_is_object");
            assertThat(harness.observed()).anySatisfy(constraint -> assertThat(functionsWithoutOpaque(constraint.getExpression())).contains("phase3_probe_is_object"));
            assertThat(harness.execute(predicate, true).getOnlyColumnAsSet())
                    .as(residual).isEqualTo(harness.execute(predicate, false).getOnlyColumnAsSet()).containsExactly(1L);
        }
    }

    @Test
    void testNondeterministicResidualRemains()
    {
        assertThat(filters(harness.plan("phase3_probe_is_object(doc) AND random() < 0.5", true, true)))
                .contains("random").doesNotContain("phase3_probe_is_object");
    }

    @Test
    void testComplexPushdownOffRetainsFilter()
    {
        assertThat(filters(harness.plan("doc['a'] = CAST(7 AS VARIANT)", false, false))).contains("doc");
        assertThat(harness.observed()).allSatisfy(constraint -> assertThat(functionsWithoutOpaque(constraint.getExpression())).doesNotContain("$operator$subscript"));
    }

    @Test
    void testNoResultDoesNotMeanError()
    {
        ConnectorExpression expression = harness.expression("CAST(doc['a'] AS BIGINT) = 7");
        ConnectorExpressionEvaluator evaluator = new InternalConnectorExpressionEvaluator(harness.tester().getPlannerContext());
        var prepared = evaluator.prepare(harness.tester().getDefaultSession().toConnectorSession(), expression);
        String variable = prepared.getArguments().iterator().next();
        assertThat(prepared.getArguments()).hasSize(1);
        Map<String, NullableValue> valid = Map.of(variable, NullableValue.of(VARIANT, Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7)))));
        assertThat(prepared.tryEvaluate(valid)).isEqualTo(new ConnectorExpressionEvaluator.EvaluationResult.Value(true));
        assertThat(prepared.tryEvaluate(Map.of())).isInstanceOf(ConnectorExpressionEvaluator.EvaluationResult.NoResult.class);
        assertThat(ConnectorExpressionEvaluator.NO_OP.prepare(harness.tester().getDefaultSession().toConnectorSession(), expression).tryEvaluate(valid))
                .isInstanceOf(ConnectorExpressionEvaluator.EvaluationResult.NoResult.class);
        assertThat(prepared.tryEvaluate(Map.of(variable, NullableValue.of(VARIANT, Variant.ofLong(7)))))
                .isInstanceOf(ConnectorExpressionEvaluator.EvaluationResult.NoResult.class);
        assertThat(prepared.tryEvaluate(Map.of(variable, NullableValue.of(
                VARIANT,
                Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofDecimal(new BigDecimal("1000000000000000000000000000000"))))))))
                .isInstanceOf(ConnectorExpressionEvaluator.EvaluationResult.NoResult.class);
    }

    @Test
    void testTryCastDoesNotCatchSubscriptFailure()
    {
        ConnectorExpression expression = harness.expression("TRY_CAST(doc['a'] AS BIGINT) = 7");
        var prepared = new InternalConnectorExpressionEvaluator(harness.tester().getPlannerContext())
                .prepare(harness.tester().getDefaultSession().toConnectorSession(), expression);
        String variable = prepared.getArguments().iterator().next();
        assertThat(prepared.tryEvaluate(Map.of(variable, NullableValue.of(VARIANT, Variant.ofLong(7)))))
                .isInstanceOf(ConnectorExpressionEvaluator.EvaluationResult.NoResult.class);
        assertThat(prepared.tryEvaluate(Map.of(variable, NullableValue.of(VARIANT, Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofString("bad")))))))
                .isEqualTo(new ConnectorExpressionEvaluator.EvaluationResult.Value(null));
    }
}
