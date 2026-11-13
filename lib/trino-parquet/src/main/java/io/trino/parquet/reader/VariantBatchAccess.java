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

import io.trino.parquet.VariantPath;
import io.trino.spi.variant.Variant;

import java.util.List;
import java.util.Optional;

/**
 * A borrowed view of one requested VARIANT path union, in original batch positions.
 */
public interface VariantBatchAccess
{
    /**
     * One result for a key path, or one per list element. An empty list has no candidates.
     */
    List<VariantPathProbe> probe(VariantPath path, int originalPosition);

    /**
     * Reconstructs this view's requested path union, caching only rows requested here. Empty means SQL NULL.
     */
    Optional<Variant> reference(int originalPosition);
}
