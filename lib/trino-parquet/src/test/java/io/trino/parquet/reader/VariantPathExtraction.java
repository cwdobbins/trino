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
import io.airlift.slice.Slice;
import io.trino.parquet.VariantPath;
import io.trino.parquet.VariantPath.Step;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Variant;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * The oracle for pruned variant reads: what a consumer can observe about a path in a document, computed with the
 * variant SPI's accessors only, independently of the Parquet reader and of pruning.
 */
public final class VariantPathExtraction
{
    private VariantPathExtraction() {}

    /**
     * What a consumer can observe about a path in a document.
     */
    public sealed interface Extraction
    {
        /**
         * The path reached a value, which may be a variant null. Compared by logical equality, see {@link #same}.
         */
        record Value(Variant value)
                implements Extraction
        {
            public Value
            {
                requireNonNull(value, "value is null");
            }
        }

        /**
         * A key step met an object without that key.
         */
        record Missing()
                implements Extraction {}

        /**
         * The step at {@code stepIndex} was applied to a value of the wrong shape: a key step to a non-object or an
         * element step to a non-array. {@code actualKind} is the name Trino's subscript errors give the value
         * ("VARIANT value is X, not an object").
         */
        record WrongShape(int stepIndex, String actualKind)
                implements Extraction
        {
            public WrongShape
            {
                requireNonNull(actualKind, "actualKind is null");
            }
        }

        /**
         * The document itself is SQL NULL.
         */
        record SqlNull()
                implements Extraction {}

        /**
         * An element step on an array: one extraction of the rest of the path per element, in order.
         */
        record Elements(List<Extraction> perElement)
                implements Extraction
        {
            public Elements
            {
                perElement = ImmutableList.copyOf(perElement);
            }
        }
    }

    /**
     * Extracts {@code path} from {@code document}; a null document is SQL NULL.
     */
    public static Extraction extract(Variant document, VariantPath path)
    {
        if (document == null) {
            return new Extraction.SqlNull();
        }
        return extract(document, path.steps(), 0);
    }

    private static Extraction extract(Variant value, List<Step> steps, int index)
    {
        if (index == steps.size()) {
            return new Extraction.Value(value);
        }
        Step step = steps.get(index);
        if (step.isElement()) {
            if (value.basicType() != BasicType.ARRAY) {
                return new Extraction.WrongShape(index, kind(value));
            }
            ImmutableList.Builder<Extraction> elements = ImmutableList.builder();
            int length = value.getArrayLength();
            for (int i = 0; i < length; i++) {
                elements.add(extract(value.getArrayElement(i), steps, index + 1));
            }
            return new Extraction.Elements(elements.build());
        }
        if (value.basicType() != BasicType.OBJECT) {
            return new Extraction.WrongShape(index, kind(value));
        }
        Optional<Variant> field = value.getObjectField(utf8Slice(step.key().orElseThrow()));
        if (field.isEmpty()) {
            return new Extraction.Missing();
        }
        return extract(field.get(), steps, index + 1);
    }

    /**
     * The names {@code VariantOperators.variantTypeName} gives a value in Trino's subscript errors.
     */
    static String kind(Variant value)
    {
        return switch (value.basicType()) {
            case PRIMITIVE -> value.primitiveType().name().toLowerCase(ENGLISH);
            case SHORT_STRING -> "string";
            case OBJECT, ARRAY -> value.basicType().name().toLowerCase(ENGLISH);
        };
    }

    /**
     * Whether two extractions are the same observation: values are compared by {@link #sameValue}, at every level of
     * {@link Extraction.Elements}, everything else structurally.
     */
    public static boolean same(Extraction left, Extraction right)
    {
        return switch (left) {
            case Extraction.Value(Variant value) -> right instanceof Extraction.Value(Variant other) && sameValue(value, other);
            case Extraction.Elements(List<Extraction> elements) -> {
                if (!(right instanceof Extraction.Elements(List<Extraction> others)) || elements.size() != others.size()) {
                    yield false;
                }
                for (int i = 0; i < elements.size(); i++) {
                    if (!same(elements.get(i), others.get(i))) {
                        yield false;
                    }
                }
                yield true;
            }
            case Extraction.Missing _, Extraction.WrongShape _, Extraction.SqlNull _ -> left.equals(right);
        };
    }

    /**
     * Whether two values are the same as a query sees them: logically equal ({@link Variant#equals}, which equates
     * numbers across types and decimal scales), rendered to identical JSON text, and of the same kind at every node.
     */
    static boolean sameValue(Variant left, Variant right)
    {
        return left.equals(right) &&
                ShreddedVariantTestUtils.toJson(left).equals(ShreddedVariantTestUtils.toJson(right)) &&
                sameKinds(left, right);
    }

    // The same kind at every node, and for decimals the same scale (CAST AS VARCHAR shows the scale)
    private static boolean sameKinds(Variant left, Variant right)
    {
        if (!kind(left).equals(kind(right))) {
            return false;
        }
        return switch (left.basicType()) {
            case OBJECT -> {
                Set<Slice> names = left.objectFieldNames().collect(toImmutableSet());
                if (!names.equals(right.objectFieldNames().collect(toImmutableSet()))) {
                    yield false;
                }
                for (Slice name : names) {
                    if (!sameKinds(left.getObjectField(name).orElseThrow(), right.getObjectField(name).orElseThrow())) {
                        yield false;
                    }
                }
                yield true;
            }
            case ARRAY -> {
                int length = left.getArrayLength();
                if (length != right.getArrayLength()) {
                    yield false;
                }
                for (int i = 0; i < length; i++) {
                    if (!sameKinds(left.getArrayElement(i), right.getArrayElement(i))) {
                        yield false;
                    }
                }
                yield true;
            }
            case PRIMITIVE -> switch (left.primitiveType()) {
                case DECIMAL4, DECIMAL8, DECIMAL16 -> left.getDecimal().scale() == right.getDecimal().scale();
                default -> true;
            };
            case SHORT_STRING -> true;
        };
    }

    /**
     * A readable form of an extraction, with values as JSON text, for failure messages.
     */
    public static String describe(Extraction extraction)
    {
        return switch (extraction) {
            case Extraction.Value(Variant value) -> "Value(" + ShreddedVariantTestUtils.toJson(value) + ")";
            case Extraction.Elements(List<Extraction> elements) -> "Elements" + elements.stream().map(VariantPathExtraction::describe).toList();
            case Extraction.Missing _, Extraction.WrongShape _, Extraction.SqlNull _ -> extraction.toString();
        };
    }
}
