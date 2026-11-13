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

import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.PageBuilder;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.variant.Variant;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PlanTester;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Uses the real SQL planner and evaluator, with a recording connector instead of a filesystem.
 */
final class VariantExpressionTestHarness
        implements AutoCloseable
{
    private final PlanTester tester;
    private final List<Constraint> observed = new ArrayList<>();
    private boolean claimInLatePass;

    public VariantExpressionTestHarness()
    {
        tester = PlanTester.create(testSessionBuilder().setCatalog("variant_probe").setSchema("s").build());
        tester.installPlugin(new Plugin()
        {
            @Override
            public Set<Class<?>> getFunctions()
            {
                return Set.of(ProbeFunctions.class);
            }
        });
        tester.createCatalog("variant_probe", withVariantPages(MockConnectorFactory.builder()
                .withName("variant_probe")
                .withGetColumns(_ -> List.of(new ColumnMetadata("id", BIGINT), new ColumnMetadata("doc", VARIANT), new ColumnMetadata("flag", BOOLEAN)))
                .withApplyFilter((_, handle, constraint) -> {
                    observed.add(constraint);
                    // Test-only enforcement: withData applies this total predicate when this mode is enabled.
                    if (!claimInLatePass || !functions(constraint.getExpression()).contains("$engine_expression") ||
                            !functionsWithoutOpaque(constraint.getExpression()).contains("phase3_probe_is_object")) {
                        return Optional.empty();
                    }
                    return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary(), removeClaimAndOpaque(constraint.getExpression()), false));
                })
                .build()), Map.of());
    }

    // InMemoryRecordSet does not support Variant's retained size. Use actual typed blocks.
    private ConnectorFactory withVariantPages(MockConnectorFactory factory)
    {
        return new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return factory.getName();
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                Connector delegate = factory.create(catalogName, config, context);
                return new Connector()
                {
                    @Override
                    public void shutdown()
                    {
                        delegate.shutdown();
                    }

                    @Override
                    public ConnectorTransactionHandle beginTransaction(IsolationLevel level, boolean readOnly, boolean autoCommit)
                    {
                        return delegate.beginTransaction(level, readOnly, autoCommit);
                    }

                    @Override
                    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
                    {
                        return delegate.getMetadata(session, transaction);
                    }

                    @Override
                    public ConnectorSplitManager getSplitManager()
                    {
                        return delegate.getSplitManager();
                    }

                    @Override
                    public ConnectorPageSourceProvider getPageSourceProvider()
                    {
                        return new ConnectorPageSourceProvider()
                        {
                            @Override
                            public ConnectorPageSource createPageSource(
                                    ConnectorTransactionHandle transaction,
                                    ConnectorSession session,
                                    ConnectorSplit split,
                                    ConnectorTableHandle table,
                                    Optional<ConnectorTableCredentials> credentials,
                                    List<ColumnHandle> columns,
                                    DynamicFilter dynamicFilter,
                                    MemoryContext memoryContext)
                            {
                                List<MockConnectorColumnHandle> requested = columns.stream().map(MockConnectorColumnHandle.class::cast).toList();
                                PageBuilder page = new PageBuilder(requested.stream().map(MockConnectorColumnHandle::type).toList());
                                for (List<?> row : rows()) {
                                    if (claimInLatePass && !ProbeFunctions.isObject((Variant) row.get(1))) {
                                        continue;
                                    }
                                    page.declarePosition();
                                    for (int channel = 0; channel < requested.size(); channel++) {
                                        MockConnectorColumnHandle column = requested.get(channel);
                                        int index = List.of("id", "doc", "flag").indexOf(column.name());
                                        writeNativeValue(column.type(), page.getBlockBuilder(channel), row.get(index));
                                    }
                                }
                                return new FixedPageSource(List.of(page.build()));
                            }
                        };
                    }
                };
            }
        };
    }

    public PlanTester tester()
    {
        return tester;
    }

    public List<Constraint> observed()
    {
        return List.copyOf(observed);
    }

    public Plan plan(String predicate, boolean complexPushdown, boolean claimInLatePass)
    {
        observed.clear();
        this.claimInLatePass = claimInLatePass;
        Session session = Session.builder(tester.getDefaultSession())
                .setSystemProperty("complex_expression_pushdown", Boolean.toString(complexPushdown))
                .build();
        return tester.inTransaction(session, transaction -> tester.createPlan(transaction, "SELECT id FROM docs WHERE " + predicate));
    }

    public ConnectorExpression expression(String predicate)
    {
        plan(predicate, true, false);
        return observed.stream()
                .map(Constraint::getExpression)
                .filter(expression -> !functions(expression).contains("$engine_expression"))
                .filter(expression -> !expression.equals(Constant.TRUE))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("No transparent expression: " + predicate));
    }

    public MaterializedResult execute(String predicate, boolean claimInLatePass)
    {
        observed.clear();
        this.claimInLatePass = claimInLatePass;
        return tester.executePlan(session -> tester.createPlan(session, "SELECT id FROM docs WHERE " + predicate + " ORDER BY id"), new PlanTester.MaterializedResultOutput());
    }

    public static List<String> functions(ConnectorExpression expression)
    {
        return walk(expression, false).filter(Call.class::isInstance).map(Call.class::cast)
                .map(call -> call.getFunctionName().getName()).toList();
    }

    public static List<String> functionsWithoutOpaque(ConnectorExpression expression)
    {
        return walk(expression, true).filter(Call.class::isInstance).map(Call.class::cast)
                .map(call -> call.getFunctionName().getName()).toList();
    }

    public static String filters(Plan plan)
    {
        return nodes(plan.getRoot()).filter(FilterNode.class::isInstance).map(FilterNode.class::cast)
                .map(filter -> filter.getPredicate().toString()).toList().toString();
    }

    private static Stream<ConnectorExpression> walk(ConnectorExpression expression, boolean omitOpaque)
    {
        if (omitOpaque && expression instanceof Call call && call.getFunctionName().getName().equals("$engine_expression")) {
            return Stream.empty();
        }
        return Stream.concat(Stream.of(expression), expression.getChildren().stream().flatMap(child -> walk(child, omitOpaque)));
    }

    private static Stream<PlanNode> nodes(PlanNode node)
    {
        return Stream.concat(Stream.of(node), node.getSources().stream().flatMap(VariantExpressionTestHarness::nodes));
    }

    private static ConnectorExpression removeClaimAndOpaque(ConnectorExpression expression)
    {
        if (!(expression instanceof Call call)) {
            return expression;
        }
        if (call.getFunctionName().getName().equals("phase3_probe_is_object") || call.getFunctionName().getName().equals("$engine_expression")) {
            return Constant.TRUE;
        }
        if (!call.getFunctionName().equals(AND_FUNCTION_NAME)) {
            return expression;
        }
        List<ConnectorExpression> remaining = call.getArguments().stream()
                .map(VariantExpressionTestHarness::removeClaimAndOpaque)
                .filter(argument -> !argument.equals(Constant.TRUE)).toList();
        if (remaining.isEmpty()) {
            return Constant.TRUE;
        }
        if (remaining.size() == 1) {
            return remaining.getFirst();
        }
        return new Call(BOOLEAN, AND_FUNCTION_NAME, remaining);
    }

    private static List<List<?>> rows()
    {
        return List.of(
                List.of(1L, Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7))), true),
                List.of(2L, Variant.ofLong(5), false),
                List.of(3L, Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(8))), true));
    }

    @Override
    public void close()
    {
        tester.close();
    }

    public static final class ProbeFunctions
    {
        private ProbeFunctions() {}

        @ScalarFunction("phase3_probe_is_object")
        @SqlType("boolean")
        public static boolean isObject(@SqlNullable @SqlType("variant") Variant value)
        {
            return value != null && value.basicType() == OBJECT;
        }
    }
}
