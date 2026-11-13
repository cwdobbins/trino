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

import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.spi.variant.Variant;
import jakarta.annotation.Nullable;

import java.util.List;

/**
 * Runtime-only callback; neither this callback nor its batch views may be serialized in scan handles.
 */
public interface VariantPredicateEvaluator
{
    enum Decision
    {
        FALSE((byte) 0),
        TRUE((byte) 1),
        SQL_NULL((byte) 2),
        UNKNOWN((byte) 3);

        private final byte code;

        Decision(byte code)
        {
            this.code = code;
        }

        public byte code()
        {
            return code;
        }

        public static Decision fromCode(byte code)
        {
            return switch (code) {
                case 0 -> FALSE;
                case 1 -> TRUE;
                case 2 -> SQL_NULL;
                case 3 -> UNKNOWN;
                default -> throw new IllegalArgumentException("Invalid VARIANT decision: " + code);
            };
        }
    }

    Decision evaluate(VariantBatchAccess batch, int originalPosition);

    /**
     * Complete exact evaluation, or conservative UNKNOWN for a native hint, on unsupported physical layouts.
     */
    Decision evaluateReference(@Nullable Variant value);

    /**
     * Returns true only when row-group metadata proves this predicate cannot match any row.
     */
    default boolean cannotMatchRowGroup(String sourceColumn, long rowCount, List<ColumnChunkMetadata> columns)
    {
        return false;
    }
}
