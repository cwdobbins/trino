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

import io.trino.parquet.reader.VariantPredicateEvaluator;
import io.trino.spi.connector.ColumnHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.Objects.requireNonNull;

/**
 * Worker-local request only: never serialize it or put it in a table, split, predicate domain or delete schema.
 */
public record IcebergVariantPredicateColumn(IcebergColumnHandle source, VariantPredicateEvaluator evaluator)
        implements ColumnHandle
{
    public IcebergVariantPredicateColumn
    {
        requireNonNull(source, "source is null");
        requireNonNull(evaluator, "evaluator is null");
        checkArgument(source.isBaseColumn() && source.getType().equals(VARIANT), "Expected a top-level VARIANT source");
    }
}
