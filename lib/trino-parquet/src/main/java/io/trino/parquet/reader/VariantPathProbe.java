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

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * A conservative path observation. Blocks and positions are borrowed until the reader advances.
 */
public final class VariantPathProbe
{
    public enum State
    {
        SQL_NULL,
        MISSING,
        WRONG_SHAPE,
        TYPED_VALUE,
        UNRESOLVED,
    }

    private final Runnable checkValid;
    private final State state;
    private final String columnPath;
    @Nullable
    private final Type type;
    @Nullable
    private final Block block;
    private final int position;

    public VariantPathProbe(Runnable checkValid, State state, String columnPath, @Nullable Type type, @Nullable Block block, int position)
    {
        this.checkValid = requireNonNull(checkValid, "checkValid is null");
        this.state = requireNonNull(state, "state is null");
        this.columnPath = requireNonNull(columnPath, "columnPath is null");
        checkState((state == State.TYPED_VALUE) == (type != null) && (type != null) == (block != null), "Invalid typed probe");
        this.type = type;
        this.block = block;
        this.position = position;
    }

    public State state()
    {
        checkValid.run();
        return state;
    }

    public Type type()
    {
        checkTyped();
        return type;
    }

    public Block block()
    {
        checkTyped();
        return block;
    }

    public int position()
    {
        checkTyped();
        return position;
    }

    /**
     * Uses exactly the ordinary reader's primitive conversion, without constructing enclosing objects.
     */
    public Variant value()
    {
        checkTyped();
        return ShreddedVariantReader.scalarToVariant(columnPath, type, block, position);
    }

    private void checkTyped()
    {
        checkValid.run();
        checkState(state == State.TYPED_VALUE, "Probe is not a typed value: %s", state);
    }
}
