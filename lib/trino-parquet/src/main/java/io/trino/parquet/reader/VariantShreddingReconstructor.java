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

import io.airlift.slice.Slice;
import io.trino.parquet.reader.ShreddedValue.ShreddedArray;
import io.trino.parquet.reader.ShreddedValue.ShreddedObject;
import io.trino.parquet.reader.ShreddedValue.ShreddedScalar;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import io.trino.spi.variant.VariantDecoder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reconstructs an unshredded Variant from its shredded parts, following the reconstruction rules of
 * the <a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">Parquet
 * Variant shredding spec</a>.
 * <p>
 * The result is rebuilt with {@link Variant#ofObject}/{@link Variant#ofArray}/{@code Variant.of*},
 * which merge nested metadata dictionaries and remap field ids, so the returned Variant is
 * self-contained and independent of the shredded column's original metadata. The original metadata
 * is still required to interpret the non-shredded fields carried in a partially shredded object's
 * {@code value} column.
 */
public final class VariantShreddingReconstructor
{
    private VariantShreddingReconstructor() {}

    /**
     * Reconstructs the Variant for one row, or {@link Optional#empty()} when the value is missing:
     * {@code value} and {@code typed_value} both null. Only an object field may stay missing (it is
     * absent from the object); callers that require a value, the top level and array elements,
     * substitute {@link Variant#NULL_VALUE}.
     */
    public static Optional<Variant> reconstruct(ShreddedValue node, Metadata metadata)
    {
        return switch (node) {
            case ShreddedScalar scalar -> {
                if (scalar.typedValue().isPresent()) {
                    yield scalar.typedValue();
                }
                yield scalar.value().map(value -> variantFrom(metadata, value));
            }
            case ShreddedObject object -> reconstructObject(object, metadata);
            case ShreddedArray array -> reconstructArray(array, metadata);
        };
    }

    /**
     * Wraps the bytes of a {@code value} column. The value's own encoding says how long it is, and bytes
     * after it are not part of it: parquet-java's variant writer writes a shredded object field's
     * {@code value} as a view of its parent's buffer that runs on past the field's end. Left in, those
     * bytes make the value as long as the whole object and break remapping its field ids.
     */
    private static Variant variantFrom(Metadata metadata, Slice value)
    {
        int size = VariantDecoder.valueSize(value, 0);
        if (size > 0 && size < value.length()) {
            value = value.slice(0, size);
        }
        return Variant.from(metadata, value);
    }

    private static Optional<Variant> reconstructObject(ShreddedObject object, Metadata metadata)
    {
        if (object.shreddedFields().isEmpty()) {
            return object.value().map(value -> variantFrom(metadata, value));
        }

        // The insertion order does not matter: ofObject sorts fields by name.
        Map<Slice, Variant> fields = new LinkedHashMap<>();
        for (Map.Entry<Slice, ShreddedValue> shreddedField : object.shreddedFields().get().entrySet()) {
            reconstruct(shreddedField.getValue(), metadata)
                    .ifPresent(value -> fields.put(shreddedField.getKey(), value));
        }

        // A partially shredded object carries the remaining fields in value; the spec guarantees
        // value never repeats a shredded field.
        // If value is not an object (INVALID per the spec) typed_value alone is used.
        if (object.value().isPresent()) {
            Variant remaining = variantFrom(metadata, object.value().get());
            if (remaining.basicType() == BasicType.OBJECT) {
                remaining.objectFields()
                        .forEach(field -> fields.putIfAbsent(metadata.get(field.fieldId()), field.value()));
            }
        }

        return Optional.of(Variant.ofObject(fields));
    }

    private static Optional<Variant> reconstructArray(ShreddedArray array, Metadata metadata)
    {
        if (array.typedElements().isEmpty()) {
            return array.value().map(value -> variantFrom(metadata, value));
        }

        List<ShreddedValue> shreddedElements = array.typedElements().get();
        List<Variant> elements = new ArrayList<>(shreddedElements.size());
        for (ShreddedValue element : shreddedElements) {
            // Array elements are never missing: value and typed_value both null reads as a Variant null
            elements.add(reconstruct(element, metadata).orElse(Variant.NULL_VALUE));
        }
        return Optional.of(Variant.ofArray(elements));
    }
}
