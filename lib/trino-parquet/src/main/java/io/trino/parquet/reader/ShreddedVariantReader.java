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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.parquet.VariantPath;
import io.trino.parquet.reader.ShreddedValue.ShreddedArray;
import io.trino.parquet.reader.ShreddedValue.ShreddedObject;
import io.trino.parquet.reader.ShreddedValue.ShreddedScalar;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import jakarta.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTypeUtils.shreddedLayoutException;
import static io.trino.parquet.ShreddedVariantField.METADATA;
import static io.trino.parquet.ShreddedVariantField.TYPED_VALUE;
import static io.trino.parquet.ShreddedVariantField.VALUE;
import static io.trino.parquet.reader.VariantPathProbe.State.MISSING;
import static io.trino.parquet.reader.VariantPathProbe.State.UNRESOLVED;
import static io.trino.parquet.reader.VariantPathProbe.State.WRONG_SHAPE;
import static io.trino.parquet.reader.VariantShreddingReconstructor.reconstruct;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;

/**
 * Reads a shredded Variant column that has been materialized (by the standard nested Parquet
 * readers) as a struct block mirroring the Parquet shredding layout, and reconstructs the
 * unshredded Variant for each row via {@link VariantShreddingReconstructor}.
 * <p>
 * The struct type of a Variant node is a {@link RowType} with a {@code value} field (optional
 * {@code VARBINARY}) and a {@code typed_value} field; the top-level node additionally has a
 * {@code metadata} field. A {@code typed_value} of {@link RowType} is a shredded object (one field
 * per shredded key, each itself a node), of {@link io.trino.spi.type.ArrayType} is a shredded array
 * (element is a node), and otherwise is a shredded scalar.
 */
public final class ShreddedVariantReader
{
    private ShreddedVariantReader() {}

    /**
     * Reconstructs the Variant for every position of a shredded Variant column, where empty entries
     * are SQL nulls.
     */
    public static List<Optional<Variant>> reconstructColumn(String columnPath, RowType structType, Block structBlock, int positionCount)
    {
        return reconstructColumn(columnPath, structType, structBlock, null, 0, positionCount);
    }

    /**
     * Reconstructs the Variant for the selected positions of a shredded Variant column only: entry {@code i} is the
     * Variant of position {@code positions[offset + i]} of {@code structBlock}, so the result is what reconstructing
     * every position and then selecting would give, without decoding the unselected rows. A null {@code positions}
     * selects positions {@code 0} to {@code count - 1}. Positions may repeat and be in any order. The node readers are
     * built once for the batch and read only at the selected positions, as for a whole column.
     */
    public static List<Optional<Variant>> reconstructColumn(String columnPath, RowType structType, Block structBlock, @Nullable int[] positions, int offset, int count)
    {
        int metadataIndex = fieldIndex(structType, METADATA);
        if (metadataIndex < 0) {
            throw shreddedLayoutException(columnPath, "metadata is missing");
        }
        List<Block> fields = getRowFieldsFromBlock(structBlock);
        Block metadataBlock = fields.get(metadataIndex);
        NodeReader root = new NodeReader(columnPath, structType, fields);

        ImmutableList.Builder<Optional<Variant>> variants = ImmutableList.builderWithExpectedSize(count);
        for (int row = 0; row < count; row++) {
            int position = positions == null ? row : positions[offset + row];
            if (structBlock.isNull(position)) {
                variants.add(Optional.empty());
                continue;
            }
            Metadata metadata = decodeBytes(columnPath, () -> Metadata.from(VARBINARY.getSlice(metadataBlock, position)));
            ShreddedValue node = root.read(position);
            // A present group with value and typed_value both null is a Variant null; only a null group is SQL NULL
            variants.add(Optional.of(decodeBytes(columnPath, () -> reconstruct(node, metadata)).orElse(Variant.NULL_VALUE)));
        }
        return variants.build();
    }

    /**
     * The exception the SPI's variant code raises through {@code VariantUtils.verify} when bytes are inconsistent. The class
     * is package-private in the SPI, so it can only be matched by name; the SPI has no Guava, so Guava's VerifyException
     * is never what it throws. A test pins that this class exists.
     */
    static final String SPI_VERIFY_EXCEPTION = "io.trino.spi.variant.VerifyException";

    /**
     * Runs a decoder of bytes read from the file (the variant metadata, or a {@code value} during reconstruction).
     * Only the exceptions the Variant and Metadata decoders raise on malformed bytes are translated, keeping the
     * cause; anything else is a bug in this reader and propagates as it is.
     */
    static <T> T decodeBytes(String columnPath, Supplier<T> decoder)
    {
        try {
            return decoder.get();
        }
        catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException e) {
            throw shreddedLayoutException(columnPath, "invalid variant encoding in the file: " + e.getMessage(), e);
        }
        catch (RuntimeException e) {
            // Matched by identity, not by catching RuntimeException: anything else is a bug in this reader and propagates as it is
            if (e.getClass().getName().equals(SPI_VERIFY_EXCEPTION)) {
                throw shreddedLayoutException(columnPath, "invalid variant encoding in the file: " + e.getMessage(), e);
            }
            throw e;
        }
    }

    /**
     * Builds the {@link ShreddedValue} for a single node at {@code position}. Any {@code metadata}
     * field is ignored; only {@code value} and {@code typed_value} are read.
     */
    static ShreddedValue walkNode(String path, RowType nodeType, List<Block> nodeFields, int position)
    {
        return new NodeReader(path, nodeType, nodeFields).read(position);
    }

    /**
     * One Variant node of the layout over the blocks of one batch. The views a nested node is read through (the field
     * blocks of a shredded object, the columnar form of a shredded array and its elements' field blocks) are built from
     * the whole batch's blocks, so they are built once per batch, on first use, and then shared by every row: a block the
     * nested readers wrapped in a dictionary (as they do below a struct with a null in the batch) costs time in
     * proportion to the batch to view, and viewing it once per row made a batch quadratic. Positions are those of the
     * node's own blocks: a row's position at the top, and an element's position in the elements block below an array.
     * Layout errors are raised at the same point of the walk as when every row built its own views.
     */
    static final class NodeReader
    {
        private final String path;
        private final RowType nodeType;
        private final List<Block> nodeFields;
        private final int valueIndex;
        private final int typedValueIndex;
        private final LongConsumer retain;

        // a shredded object: its fields' blocks, and one reader per field, each built on first use
        private List<Block> objectFields;
        private NodeReader[] objectFieldReaders;
        private Slice[] objectFieldNames;

        // a shredded array: its columnar view and its element reader, built on first use
        private ColumnarArray array;
        private NodeReader elementReader;

        private int viewsBuilt;

        NodeReader(String path, RowType nodeType, List<Block> nodeFields)
        {
            this(path, nodeType, nodeFields, _ -> {});
        }

        NodeReader(String path, RowType nodeType, List<Block> nodeFields, LongConsumer retain)
        {
            this.retain = retain;
            retain.accept(instanceSize(NodeReader.class) + estimatedSizeOf(nodeFields, Block::getRetainedSizeInBytes));
            this.path = path;
            this.nodeType = nodeType;
            this.nodeFields = nodeFields;
            this.valueIndex = fieldIndex(nodeType, VALUE);
            this.typedValueIndex = fieldIndex(nodeType, TYPED_VALUE);
        }

        ShreddedValue read(int position)
        {
            Optional<Slice> value = sliceField(position);

            if (typedValueIndex < 0) {
                return new ShreddedScalar(value, Optional.empty());
            }
            Type typedValueType = nodeType.getFields().get(typedValueIndex).getType();
            Block typedValueBlock = nodeFields.get(typedValueIndex);
            boolean typedValueNull = typedValueBlock.isNull(position);

            if (typedValueType instanceof RowType objectType) {
                if (typedValueNull) {
                    return new ShreddedObject(value, Optional.empty());
                }
                prepareObject(typedValueBlock, objectType);
                Map<Slice, ShreddedValue> shreddedFields = new LinkedHashMap<>();
                List<RowType.Field> fields = objectType.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = fields.get(i);
                    if (objectFieldNames[i] == null) {
                        objectFieldNames[i] = utf8Slice(field.getName().orElseThrow());
                        retain.accept(objectFieldNames[i].getRetainedSize());
                    }
                    Slice name = objectFieldNames[i];
                    if (!(field.getType() instanceof RowType childType)) {
                        throw shreddedLayoutException(path, "object field '%s' is not a Variant node: %s".formatted(name.toStringUtf8(), field.getType()));
                    }
                    objectReader(i, childType, name.toStringUtf8());
                    shreddedFields.put(name, objectFieldReaders[i].read(position));
                }
                return new ShreddedObject(value, Optional.of(shreddedFields));
            }

            if (typedValueType instanceof ArrayType arrayType) {
                if (typedValueNull) {
                    return new ShreddedArray(value, Optional.empty());
                }
                prepareArray(typedValueBlock, arrayType);
                int offset = array.getOffset(position);
                int length = array.getLength(position);
                ImmutableList.Builder<ShreddedValue> elements = ImmutableList.builderWithExpectedSize(length);
                for (int i = 0; i < length; i++) {
                    elements.add(elementReader.read(offset + i));
                }
                return new ShreddedArray(value, Optional.of(elements.build()));
            }

            if (typedValueNull) {
                return new ShreddedScalar(value, Optional.empty());
            }
            return new ShreddedScalar(value, Optional.of(scalarToVariant(path + "." + TYPED_VALUE, typedValueType, typedValueBlock, position)));
        }

        void probe(List<VariantPath.Step> steps, int step, int position, boolean requiredValue, Runnable checkValid, ImmutableList.Builder<VariantPathProbe> result)
        {
            // A fallback at any ancestor can supply an absent shredded key. Never infer absence from a null leaf alone.
            if (valueIndex >= 0 && !nodeFields.get(valueIndex).isNull(position)) {
                result.add(new VariantPathProbe(checkValid, UNRESOLVED, path, null, null, position));
                return;
            }
            if (typedValueIndex < 0 || nodeFields.get(typedValueIndex).isNull(position)) {
                var state = MISSING;
                if (requiredValue) {
                    state = step == steps.size() ? UNRESOLVED : WRONG_SHAPE;
                }
                result.add(new VariantPathProbe(checkValid, state, path, null, null, position));
                return;
            }
            Type type = nodeType.getFields().get(typedValueIndex).getType();
            Block block = nodeFields.get(typedValueIndex);
            if (step == steps.size()) {
                if (type instanceof RowType || type instanceof ArrayType) {
                    result.add(new VariantPathProbe(checkValid, UNRESOLVED, path, null, null, position));
                }
                else {
                    result.add(new VariantPathProbe(checkValid, VariantPathProbe.State.TYPED_VALUE, path + "." + TYPED_VALUE, type, block, position));
                }
                return;
            }
            VariantPath.Step next = steps.get(step);
            if (!next.isElement() && type instanceof RowType objectType) {
                String key = next.key().orElseThrow();
                int index = fieldIndex(objectType, key);
                if (index < 0) {
                    result.add(new VariantPathProbe(checkValid, MISSING, path, null, null, position));
                    return;
                }
                prepareObject(block, objectType);
                if (!(objectType.getFields().get(index).getType() instanceof RowType childType)) {
                    throw shreddedLayoutException(path, "object field is not a Variant node: " + key);
                }
                objectReader(index, childType, key).probe(steps, step + 1, position, false, checkValid, result);
                return;
            }
            if (next.isElement() && type instanceof ArrayType arrayType) {
                prepareArray(block, arrayType);
                int offset = array.getOffset(position);
                for (int element = 0; element < array.getLength(position); element++) {
                    elementReader.probe(steps, step + 1, offset + element, true, checkValid, result);
                }
                return;
            }
            result.add(new VariantPathProbe(checkValid, WRONG_SHAPE, path, null, null, position));
        }

        private void prepareObject(Block block, RowType type)
        {
            if (objectFields == null) {
                objectFields = getRowFieldsFromBlock(block);
                objectFieldReaders = new NodeReader[type.getFields().size()];
                objectFieldNames = new Slice[type.getFields().size()];
                retain.accept(estimatedSizeOf(objectFields, Block::getRetainedSizeInBytes) + sizeOf(objectFieldReaders) + sizeOf(objectFieldNames));
                viewsBuilt++;
            }
        }

        private NodeReader objectReader(int index, RowType type, String name)
        {
            if (objectFieldReaders[index] == null) {
                objectFieldReaders[index] = new NodeReader(path + ".typed_value." + name, type, getRowFieldsFromBlock(objectFields.get(index)), retain);
                viewsBuilt++;
            }
            return objectFieldReaders[index];
        }

        private void prepareArray(Block block, ArrayType type)
        {
            if (array == null) {
                array = toColumnarArray(block);
                retain.accept(instanceSize(ColumnarArray.class) + sizeOfIntArray(array.getPositionCount() + 1) + array.getElementsBlock().getRetainedSizeInBytes());
                viewsBuilt++;
            }
            if (!(type.getElementType() instanceof RowType elementType)) {
                throw shreddedLayoutException(path, "array element is not a Variant node: " + type.getElementType());
            }
            if (elementReader == null) {
                elementReader = new NodeReader(path + ".typed_value.list.element", elementType, getRowFieldsFromBlock(array.getElementsBlock()), retain);
                viewsBuilt++;
            }
        }

        private Optional<Slice> sliceField(int position)
        {
            if (valueIndex < 0) {
                return Optional.empty();
            }
            Block block = nodeFields.get(valueIndex);
            if (block.isNull(position)) {
                return Optional.empty();
            }
            return Optional.of(VARBINARY.getSlice(block, position));
        }

        /**
         * The number of views this node and the nodes below it have built from their blocks. Each is built at most once,
         * so for one batch this is bounded by the number of object fields and arrays in the layout, whatever the rows.
         */
        @VisibleForTesting
        int viewsBuilt()
        {
            int total = viewsBuilt;
            if (objectFieldReaders != null) {
                for (NodeReader reader : objectFieldReaders) {
                    if (reader != null) {
                        total += reader.viewsBuilt();
                    }
                }
            }
            if (elementReader != null) {
                total += elementReader.viewsBuilt();
            }
            return total;
        }
    }

    static Variant scalarToVariant(String path, Type type, Block block, int position)
    {
        return switch (type) {
            case BigintType _ -> Variant.ofLong(type.getLong(block, position));
            case IntegerType _ -> Variant.ofInt(toIntExact(type.getLong(block, position)));
            case SmallintType _ -> Variant.ofShort((short) type.getLong(block, position));
            case TinyintType _ -> Variant.ofByte((byte) type.getLong(block, position));
            case RealType realType -> Variant.ofFloat(realType.getFloat(block, position));
            case DoubleType _ -> Variant.ofDouble(type.getDouble(block, position));
            case BooleanType _ -> Variant.ofBoolean(type.getBoolean(block, position));
            case DateType _ -> Variant.ofDate(toIntExact(type.getLong(block, position)));
            case DecimalType decimalType -> Variant.ofDecimal(Decimals.readBigDecimal(decimalType, block, position));
            case VarcharType _ -> Variant.ofString(type.getSlice(block, position));
            case VarbinaryType _ -> Variant.ofBinary(type.getSlice(block, position));
            case UuidType _ -> Variant.ofUuid(type.getSlice(block, position));
            // TIME(6) is held as picoseconds of the day
            case TimeType _ -> Variant.ofTimeMicrosNtz(type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND);
            case TimestampType timestampType -> timestampToVariant(timestampType, block, position);
            case TimestampWithTimeZoneType timestampType -> timestampWithTimeZoneToVariant(path, timestampType, block, position);
            default -> throw shreddedLayoutException(path, "unsupported typed_value type " + type);
        };
    }

    private static Variant timestampToVariant(TimestampType type, Block block, int position)
    {
        if (type.isShort()) {
            // precision 6 only: epoch microseconds
            return Variant.ofTimestampMicrosNtz(type.getLong(block, position));
        }
        // precision 9: epoch microseconds plus picoseconds of the microsecond
        LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
        return Variant.ofTimestampNanosNtz(timestamp.getEpochMicros() * NANOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND);
    }

    private static Variant timestampWithTimeZoneToVariant(String path, TimestampWithTimeZoneType type, Block block, int position)
    {
        // precision 6 and 9 are both long timestamps: epoch milliseconds plus picoseconds of the millisecond
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
        return switch (type.getPrecision()) {
            case 6 -> Variant.ofTimestampMicrosUtc(timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_MICROSECOND);
            case 9 -> Variant.ofTimestampNanosUtc(timestamp.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);
            default -> throw shreddedLayoutException(path, "unsupported typed_value type " + type);
        };
    }

    static int fieldIndex(RowType rowType, String name)
    {
        List<RowType.Field> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().map(name::equals).orElse(false)) {
                return i;
            }
        }
        return -1;
    }
}
