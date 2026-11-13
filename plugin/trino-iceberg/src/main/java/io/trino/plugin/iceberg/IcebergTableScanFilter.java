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

import io.trino.spi.connector.ConnectorTableHandle;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Coordinator-local scan request only: pass directly to the split manager, never serialize it.
 * The filter factory receives the selected scan schema, including snapshot-specific field names.
 * The caller must preserve row-level enforcement; this filter only excludes files proved unable to match.
 */
public record IcebergTableScanFilter(IcebergTableHandle table, Set<Integer> columnIds, Function<Schema, Predicate<DataFile>> filter)
        implements ConnectorTableHandle
{
    public IcebergTableScanFilter
    {
        requireNonNull(table, "table is null");
        columnIds = Set.copyOf(columnIds);
        requireNonNull(filter, "filter is null");
    }
}
