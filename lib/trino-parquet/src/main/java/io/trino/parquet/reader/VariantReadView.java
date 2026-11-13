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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ShreddedVariantField;
import io.trino.parquet.VariantPath;
import io.trino.spi.block.Block;
import io.trino.spi.type.RowType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.parquet.ParquetTypeUtils.shreddedLayoutException;
import static io.trino.parquet.ShreddedVariantField.METADATA;
import static io.trino.parquet.reader.ShreddedVariantReader.decodeBytes;
import static io.trino.parquet.reader.ShreddedVariantReader.fieldIndex;
import static io.trino.parquet.reader.VariantPathProbe.State.SQL_NULL;
import static io.trino.parquet.reader.VariantShreddingReconstructor.reconstruct;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.checkIndex;

/**
 * Cached object/list views and sparse reference rows for one logical schema view.
 */
final class VariantReadView
        implements VariantBatchAccess
{
    private final String columnPath;
    private final ColumnChunk chunk;
    private final Block metadata;
    private final ShreddedVariantReader.NodeReader root;
    private final Runnable checkValid;
    private final LongConsumer retain;
    private final LongConsumer referenceReconstruction;
    private final Map<Integer, Optional<Variant>> referenceRows = new HashMap<>();

    public VariantReadView(ShreddedVariantField field, ColumnChunk chunk, Runnable checkValid, LongConsumer retain, LongConsumer referenceReconstruction)
    {
        this.columnPath = field.getColumnPath();
        this.chunk = chunk;
        this.checkValid = checkValid;
        this.retain = retain;
        this.referenceReconstruction = referenceReconstruction;
        RowType type = (RowType) field.getStruct().getType();
        int metadataIndex = fieldIndex(type, METADATA);
        if (metadataIndex < 0) {
            throw shreddedLayoutException(columnPath, "metadata is missing");
        }
        List<Block> fields = getRowFieldsFromBlock(chunk.getBlock());
        this.metadata = fields.get(metadataIndex);
        this.root = new ShreddedVariantReader.NodeReader(columnPath, type, fields, retain);
        // Includes shared backing blocks conservatively; no per-row traversal of retained views.
        retain.accept(instanceSize(VariantReadView.class) + instanceSize(HashMap.class) + chunk.getBlock().getRetainedSizeInBytes());
    }

    public ColumnChunk chunk()
    {
        checkValid.run();
        return chunk;
    }

    @Override
    public List<VariantPathProbe> probe(VariantPath path, int originalPosition)
    {
        checkPosition(originalPosition);
        checkArgument(path.steps().stream().filter(VariantPath.Step::isElement).count() <= 1, "Only one list scope is supported");
        if (chunk.getBlock().isNull(originalPosition)) {
            return List.of(new VariantPathProbe(checkValid, SQL_NULL, columnPath, null, null, originalPosition));
        }
        // Keep the ordinary reader's metadata validation even when no value reconstruction is needed.
        metadata(originalPosition);
        ImmutableList.Builder<VariantPathProbe> result = ImmutableList.builder();
        root.probe(path.steps(), 0, originalPosition, true, checkValid, result);
        return result.build();
    }

    @Override
    public Optional<Variant> reference(int originalPosition)
    {
        checkPosition(originalPosition);
        Optional<Variant> value = referenceRows.get(originalPosition);
        if (value == null) {
            long start = System.nanoTime();
            value = reconstructRow(originalPosition);
            // Fallback slices can borrow an entire Parquet page. Own compact storage so each
            // cached row neither retains nor charges that shared page again.
            value = value.map(variant -> Variant.from(Metadata.from(variant.metadata().toSlice().copy()), variant.data().copy()));
            referenceReconstruction.accept(System.nanoTime() - start);
            referenceRows.put(originalPosition, value);
            // Conservative entry/table/boxed-key/Optional overhead, plus all retained variant storage.
            long bytes = 128;
            if (value.isPresent()) {
                Variant variant = value.orElseThrow();
                bytes += instanceSize(Variant.class) + instanceSize(Metadata.class)
                        + variant.data().getRetainedSize() + variant.metadata().toSlice().getRetainedSize();
            }
            retain.accept(bytes);
        }
        return value;
    }

    public Optional<Variant> reconstructRow(int position)
    {
        checkPosition(position);
        Optional<Variant> cached = referenceRows.get(position);
        if (cached != null) {
            return cached;
        }
        if (chunk.getBlock().isNull(position)) {
            return Optional.empty();
        }
        Metadata metadata = metadata(position);
        ShreddedValue value = root.read(position);
        return Optional.of(decodeBytes(columnPath, () -> reconstruct(value, metadata)).orElse(Variant.NULL_VALUE));
    }

    public int viewsBuilt()
    {
        checkValid.run();
        return root.viewsBuilt();
    }

    public int referenceRows()
    {
        checkValid.run();
        return referenceRows.size();
    }

    private Metadata metadata(int position)
    {
        return decodeBytes(columnPath, () -> Metadata.from(VARBINARY.getSlice(metadata, position)));
    }

    private void checkPosition(int position)
    {
        checkValid.run();
        checkIndex(position, chunk.getBlock().getPositionCount());
    }
}
