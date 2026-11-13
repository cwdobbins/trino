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

import com.google.common.collect.ImmutableMap;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.ShreddedVariantField;
import org.apache.parquet.column.ColumnDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Full-batch physical leaves for one VARIANT column, shared by its logical views. Chunks include
 * unselected definition/repetition levels. This owner must be reset before advancing the readers.
 * Logical views cache object/list mappings and only explicitly requested reference rows; leaf chunks are never selected blocks.
 */
final class VariantReadBatch
        implements AutoCloseable
{
    @FunctionalInterface
    public interface ChunkReader
    {
        ColumnChunk read(PrimitiveField field) throws IOException;
    }

    @FunctionalInterface
    public interface ViewReader
    {
        ColumnChunk read() throws IOException;
    }

    private static final long INSTANCE_SIZE = instanceSize(VariantReadBatch.class);
    private static final long CHUNK_INSTANCE_SIZE = instanceSize(ColumnChunk.class);
    private static final long OPTIONAL_LONG_INSTANCE_SIZE = instanceSize(OptionalLong.class);
    private static final long INTEGER_INSTANCE_SIZE = instanceSize(Integer.class);
    private final Map<ColumnDescriptor, Integer> ordinals;
    private final ColumnChunk[] chunks;
    private final ChunkReader reader;
    private final LocalMemoryContext memory;
    private final long baseRetainedSize;
    private long retainedSize;
    private int decodedLeaves;
    private boolean closed;
    private long generation;
    private long referenceReconstructionNanos;
    private long referenceReconstructedRows;
    private final Map<ShreddedVariantField, VariantReadView> views = new IdentityHashMap<>();

    public VariantReadBatch(List<PrimitiveField> fields, ChunkReader reader, LocalMemoryContext memory)
    {
        this.reader = requireNonNull(reader, "reader is null");
        this.memory = requireNonNull(memory, "memory is null");
        ImmutableMap.Builder<ColumnDescriptor, Integer> ordinals = ImmutableMap.builder();
        for (int index = 0; index < fields.size(); index++) {
            ordinals.put(fields.get(index).getDescriptor(), index);
        }
        this.ordinals = ordinals.buildOrThrow();
        this.chunks = new ColumnChunk[fields.size()];
        // Descriptor objects are owned by the reader's field registry, not by this batch.
        this.baseRetainedSize = INSTANCE_SIZE + instanceSize(IdentityHashMap.class) + sizeOf(chunks) + estimatedSizeOf(this.ordinals, _ -> 0, _ -> INTEGER_INSTANCE_SIZE);
        reset();
    }

    public ColumnChunk read(PrimitiveField field)
            throws IOException
    {
        checkState(!closed, "VARIANT batch is closed");
        Integer ordinal = ordinals.get(field.getDescriptor());
        checkArgument(ordinal != null, "Unknown VARIANT leaf: %s", field.getDescriptor());
        ColumnChunk chunk = chunks[ordinal];
        if (chunk == null) {
            chunk = requireNonNull(reader.read(field), "reader returned null");
            chunks[ordinal] = chunk;
            decodedLeaves++;
            retainedSize += CHUNK_INSTANCE_SIZE + OPTIONAL_LONG_INSTANCE_SIZE + chunk.getBlock().getRetainedSizeInBytes()
                    + sizeOf(chunk.getDefinitionLevels()) + sizeOf(chunk.getRepetitionLevels());
            memory.setBytes(retainedSize);
        }
        return chunk;
    }

    public VariantReadView view(ShreddedVariantField field, ViewReader reader)
            throws IOException
    {
        checkState(!closed, "VARIANT batch is closed");
        VariantReadView view = views.get(field);
        if (view == null) {
            long expectedGeneration = generation;
            view = new VariantReadView(
                    field,
                    reader.read(),
                    () -> checkState(!closed && generation == expectedGeneration, "VARIANT batch has expired"),
                    this::retain,
                    nanos -> {
                        referenceReconstructionNanos += nanos;
                        referenceReconstructedRows++;
                    });
            views.put(field, view);
            retain(128); // Conservative identity-map entry/table growth.
        }
        return view;
    }

    private void retain(long bytes)
    {
        retainedSize += bytes;
        memory.setBytes(retainedSize);
    }

    public long referenceReconstructionNanos()
    {
        return referenceReconstructionNanos;
    }

    public long referenceReconstructedRows()
    {
        return referenceReconstructedRows;
    }

    public int decodedLeaves()
    {
        return decodedLeaves;
    }

    public void reset()
    {
        checkState(!closed, "VARIANT batch is closed");
        generation++;
        views.clear();
        Arrays.fill(chunks, null);
        decodedLeaves = 0;
        retainedSize = baseRetainedSize;
        memory.setBytes(retainedSize);
    }

    @Override
    public void close()
    {
        generation++;
        views.clear();
        Arrays.fill(chunks, null);
        closed = true;
        memory.close();
    }
}
