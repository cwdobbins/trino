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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.variant.Variant;
import io.trino.sql.planner.InternalConnectorExpressionEvaluator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.type.VariantOperators.castToBigint;
import static io.trino.type.VariantOperators.dereference;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Pilot for the optional interpreter hint; inputs are already decoded, not end-to-end SQL.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class BenchmarkVariantExpressionEvaluator
{
    private static final int ROWS = 1024;
    private static final Variant EXPECTED = Variant.ofLong(7);

    @Param({"equality", "cast"})
    public String expression;

    @Param({"regular", "mixed"})
    public String shape;

    private VariantExpressionTestHarness harness;
    private ConnectorExpression connectorExpression;
    private ConnectorExpressionEvaluator evaluator;
    private ConnectorExpressionEvaluator.Prepared prepared;
    private List<NullableValue> values;
    private List<Map<String, NullableValue>> bindings;

    @Setup
    public void setup()
    {
        harness = new VariantExpressionTestHarness();
        connectorExpression = harness.expression(expression.equals("cast") ? "CAST(doc['a'] AS BIGINT) = 7" : "doc['a'] = CAST(7 AS VARIANT)");
        evaluator = new InternalConnectorExpressionEvaluator(harness.tester().getPlannerContext());
        prepared = prepare();
        String variable = prepared.getArguments().iterator().next();
        values = new ArrayList<>();
        bindings = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            Variant value = Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(row % 10)));
            if (shape.equals("mixed")) {
                value = switch (row % 8) {
                    case 0 -> null;
                    case 1 -> Variant.ofLong(7);
                    case 2 -> Variant.ofObject(Map.of());
                    case 3 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.NULL_VALUE));
                    case 4 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofString("bad")));
                    case 5 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofDecimal(new BigDecimal("1000000000000000000000000000000"))));
                    case 6 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofDouble(Double.NaN)));
                    default -> value;
                };
            }
            NullableValue nullable = new NullableValue(VARIANT, value);
            values.add(nullable);
            Map<String, NullableValue> binding = Map.of(variable, nullable);
            bindings.add(binding);
            if (classify(prepared.tryEvaluate(binding)) != direct(value)) {
                throw new IllegalStateException("Evaluator/direct mismatch for row " + row);
            }
        }
    }

    @TearDown
    public void tearDown()
    {
        harness.close();
    }

    @Benchmark
    public ConnectorExpressionEvaluator.Prepared prepare()
    {
        return evaluator.prepare(harness.tester().getDefaultSession().toConnectorSession(), connectorExpression);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void evaluate(Blackhole blackhole)
    {
        for (Map<String, NullableValue> binding : bindings) {
            blackhole.consume(prepared.tryEvaluate(binding));
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void direct(Blackhole blackhole)
    {
        for (NullableValue value : values) {
            blackhole.consume(direct((Variant) value.getValue()));
        }
    }

    private int direct(Variant value)
    {
        try {
            if (value == null) {
                return 2;
            }
            Variant leaf = dereference(value, utf8Slice("a"));
            if (leaf == null) {
                return 2;
            }
            if (expression.equals("cast")) {
                Long number = castToBigint(leaf);
                if (number == null) {
                    return 2;
                }
                return number == 7 ? 1 : 0;
            }
            return leaf.equals(EXPECTED) ? 1 : 0;
        }
        catch (TrinoException _) {
            return 3;
        }
    }

    private static int classify(ConnectorExpressionEvaluator.EvaluationResult result)
    {
        if (result instanceof ConnectorExpressionEvaluator.EvaluationResult.NoResult) {
            return 3;
        }
        Object value = ((ConnectorExpressionEvaluator.EvaluationResult.Value) result).value();
        if (value == null) {
            return 2;
        }
        return Boolean.TRUE.equals(value) ? 1 : 0;
    }
}
