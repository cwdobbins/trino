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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

final class TestShreddedVariantReader
{
    private static final RowType SHREDDED_NODE_BIGINT = rowType(field("value", VARBINARY), field("typed_value", BIGINT));
    private static final Slice[] ONE_NULL_SLICE = new Slice[] {null};
    private static final Long[] ONE_NULL_LONG = new Long[] {null};

    @Test
    void testTopLevelScalar()
    {
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", BIGINT));
        Block struct = RowBlock.fromFieldBlocks(1, new Block[] {
                varbinary(EMPTY_METADATA.toSlice()),
                varbinary(ONE_NULL_SLICE),
                bigint(5L),
        });
        assertThat(reconstruct(structType, struct)).isEqualTo(5L);
    }

    @Test
    void testPartiallyShreddedObject()
    {
        // top value carries the non-shredded field c; typed_value shreds a (present) and b (missing)
        Variant partialValue = Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.ofLong(3)));
        RowType typedValueType = rowType(field("a", SHREDDED_NODE_BIGINT), field("b", SHREDDED_NODE_BIGINT));
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", typedValueType));

        Block fieldA = RowBlock.fromFieldBlocks(1, new Block[] {varbinary(ONE_NULL_SLICE), bigint(1L)});
        Block fieldB = RowBlock.fromFieldBlocks(1, new Block[] {varbinary(ONE_NULL_SLICE), bigint(ONE_NULL_LONG)});
        Block typedValue = RowBlock.fromFieldBlocks(1, new Block[] {fieldA, fieldB});
        Block struct = RowBlock.fromFieldBlocks(1, new Block[] {
                varbinary(partialValue.metadata().toSlice()),
                varbinary(partialValue.data()),
                typedValue,
        });

        assertThat(reconstruct(structType, struct)).isEqualTo(ImmutableMap.of("a", 1L, "c", 3L));
    }

    @Test
    void testShreddedArray()
    {
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", new ArrayType(SHREDDED_NODE_BIGINT)));
        Block elements = RowBlock.fromFieldBlocks(2, new Block[] {varbinary(null, null), bigint(1L, 2L)});
        Block array = ArrayBlock.fromElementBlock(1, Optional.empty(), new int[] {0, 2}, elements);
        Block struct = RowBlock.fromFieldBlocks(1, new Block[] {
                varbinary(EMPTY_METADATA.toSlice()),
                varbinary(ONE_NULL_SLICE),
                array,
        });
        assertThat(reconstruct(structType, struct)).isEqualTo(ImmutableList.of(1L, 2L));
    }

    @Test
    void testViewsBuiltOncePerBatchThroughDictionary()
    {
        // An array of objects whose array block is wrapped in a dictionary, as the nested readers wrap a struct's children
        // when the batch holds a null. The ids run backwards, so a row read through the wrong position reads the wrong array.
        // Viewing a dictionary-wrapped block costs time in proportion to the batch, so each view must be built once per
        // batch, not once per row.
        int rows = 500;
        RowType elementType = rowType(field("value", VARBINARY), field("typed_value", rowType(field("a", SHREDDED_NODE_BIGINT))));
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", new ArrayType(elementType)));

        Slice[] noValues = new Slice[2 * rows];
        Long[] elementIndexes = new Long[2 * rows];
        for (int element = 0; element < 2 * rows; element++) {
            elementIndexes[element] = (long) element;
        }
        Block fieldA = RowBlock.fromFieldBlocks(2 * rows, new Block[] {varbinary(noValues), bigint(elementIndexes)});
        Block objects = RowBlock.fromFieldBlocks(2 * rows, new Block[] {fieldA});
        Block elements = RowBlock.fromFieldBlocks(2 * rows, new Block[] {varbinary(noValues), objects});
        int[] offsets = new int[rows + 1];
        int[] ids = new int[rows];
        Slice[] metadata = new Slice[rows];
        for (int row = 0; row < rows; row++) {
            offsets[row + 1] = 2 * (row + 1);
            ids[row] = rows - 1 - row;
            metadata[row] = EMPTY_METADATA.toSlice();
        }
        Block arrays = DictionaryBlock.create(rows, ArrayBlock.fromElementBlock(rows, Optional.empty(), offsets, elements), ids);
        Block struct = RowBlock.fromFieldBlocks(rows, new Block[] {varbinary(metadata), varbinary(new Slice[rows]), arrays});

        ShreddedVariantReader.NodeReader reader = new ShreddedVariantReader.NodeReader("v", structType, RowBlock.getRowFieldsFromBlock(struct));
        List<Optional<Variant>> variants = ShreddedVariantReader.reconstructColumn("v", structType, struct, rows);
        for (int row = 0; row < rows; row++) {
            long first = 2L * (rows - 1 - row);
            assertThat(variants.get(row).orElseThrow().toObject())
                    .as("row %s", row)
                    .isEqualTo(ImmutableList.of(ImmutableMap.of("a", first), ImmutableMap.of("a", first + 1)));
            reader.read(row);
        }
        // the array's view, the elements' field blocks, the objects' field blocks, and field a's blocks: once each
        assertThat(reader.viewsBuilt()).isEqualTo(4);
    }

    @Test
    void testSelectedPositionsAtAnOffset()
    {
        // six rows: typed values 0, 10, 20, a variant null, a fallback 44 in value, and 50
        RowType structType = rowType(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", BIGINT));
        Slice[] metadata = new Slice[6];
        Arrays.fill(metadata, EMPTY_METADATA.toSlice());
        Block struct = RowBlock.fromFieldBlocks(6, new Block[] {
                varbinary(metadata),
                varbinary(null, null, null, null, Variant.ofLong(44).data(), null),
                bigint(0L, 10L, 20L, null, null, 50L),
        });
        List<Optional<Variant>> whole = ShreddedVariantReader.reconstructColumn("v", structType, struct, 6);

        // the selection sits at offset 2 with junk around it, and is reversed with repeats
        int[] positions = {-1, 99, 5, 0, 5, 4, 3, 2, -7, 1};
        List<Optional<Variant>> selected = ShreddedVariantReader.reconstructColumn("v", structType, struct, positions, 2, 6);
        assertThat(selected).hasSize(6);
        int[] expectedRows = {5, 0, 5, 4, 3, 2};
        for (int i = 0; i < expectedRows.length; i++) {
            assertThat(selected.get(i)).as("entry %s", i).isEqualTo(whole.get(expectedRows[i]));
        }
        assertThat(selected.stream().map(variant -> variant.orElseThrow().toObject()).toList())
                .containsExactly(50L, 0L, 50L, 44L, null, 20L);
        // a selection of none
        assertThat(ShreddedVariantReader.reconstructColumn("v", structType, struct, positions, 9, 0)).isEmpty();
    }

    private static Object reconstruct(RowType structType, Block struct)
    {
        List<Optional<Variant>> variants = ShreddedVariantReader.reconstructColumn("v", structType, struct, struct.getPositionCount());
        return variants.get(0).orElseThrow().toObject();
    }

    private static Block varbinary(Slice... values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, values.length);
        for (Slice value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARBINARY.writeSlice(builder, value);
            }
        }
        return builder.build();
    }

    private static Block bigint(Long... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, value);
            }
        }
        return builder.build();
    }
}
