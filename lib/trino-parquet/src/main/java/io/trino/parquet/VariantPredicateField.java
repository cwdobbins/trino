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

import io.trino.parquet.reader.VariantPredicateEvaluator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.Objects.requireNonNull;

/**
 * Synthetic decision channel with real source-leaf demand and no physical column ID of its own.
 */
public final class VariantPredicateField
        extends Field
{
    private final Field source;
    private final VariantPredicateEvaluator evaluator;

    public VariantPredicateField(Field source, VariantPredicateEvaluator evaluator)
    {
        super(TINYINT, 0, 0, true);
        this.source = requireNonNull(source, "source is null");
        this.evaluator = requireNonNull(evaluator, "evaluator is null");
        checkArgument(source.getType().equals(VARIANT) && source.getRepetitionLevel() == 0, "Expected a top-level VARIANT source");
    }

    public Field source()
    {
        return source;
    }

    public VariantPredicateEvaluator evaluator()
    {
        return evaluator;
    }

    @Override
    public String toString()
    {
        return "VARIANT predicate over " + source;
    }
}
