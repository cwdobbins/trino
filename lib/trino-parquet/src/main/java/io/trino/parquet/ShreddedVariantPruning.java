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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.VariantPath.Step;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetTypeUtils.checkShreddedLayout;
import static java.util.Objects.requireNonNull;

/**
 * Computes the part of a shredded VARIANT column that must be read to extract a set of paths.
 * Extracting any requested path from a variant rebuilt from the pruned group gives the same value,
 * the same "missing" and the same shape at every step as from the full group.
 */
public final class ShreddedVariantPruning
{
    private static final String VALUE = "value";
    private static final String TYPED_VALUE = "typed_value";

    private ShreddedVariantPruning() {}

    /**
     * Validates a shredded group (one with a {@code typed_value}) with the reader's own shape rules before pruning, so a
     * pruned read rejects exactly the files a full read rejects. An unshredded group is returned unchanged.
     *
     * @param columnPath the Parquet column path of the group, for error messages
     */
    public static GroupType prune(String columnPath, GroupType variantGroup, Collection<VariantPath> paths)
    {
        requireNonNull(columnPath, "columnPath is null");
        requireNonNull(variantGroup, "variantGroup is null");
        requireNonNull(paths, "paths is null");
        checkArgument(!paths.isEmpty(), "paths is empty");
        if (!variantGroup.containsField(TYPED_VALUE)) {
            // An unshredded group is read whole; the full read applies its own checks, which accept more than the shredded rules
            return variantGroup;
        }
        checkShreddedLayout(columnPath, variantGroup);
        Set<VariantPath> normalized = VariantPath.normalize(paths);
        List<List<Step>> suffixes = new ArrayList<>();
        for (VariantPath path : normalized) {
            suffixes.add(path.steps());
        }
        if (suffixes.stream().anyMatch(List::isEmpty)) {
            return variantGroup;
        }
        return pruneNode(variantGroup, suffixes);
    }

    // suffixes are all non-empty; the node has a typed_value
    private static GroupType pruneNode(GroupType node, List<List<Step>> suffixes)
    {
        if (suffixes.stream().anyMatch(List::isEmpty)) {
            return node;
        }
        ImmutableList.Builder<Type> fields = ImmutableList.builder();
        for (Type child : node.getFields()) {
            String name = child.getName();
            if (name.equals(TYPED_VALUE)) {
                fields.add(pruneTypedValue(child, suffixes));
            }
            else {
                // metadata and value are always kept
                fields.add(child);
            }
        }
        return node.withNewFields(fields.build());
    }

    private static Type pruneTypedValue(Type typedValue, List<List<Step>> suffixes)
    {
        if (typedValue.isPrimitive()) {
            return typedValue;
        }
        GroupType group = typedValue.asGroupType();
        if (isList(group)) {
            List<List<Step>> tails = new ArrayList<>();
            for (List<Step> suffix : suffixes) {
                if (!suffix.get(0).isElement()) {
                    return typedValue;
                }
                tails.add(suffix.subList(1, suffix.size()));
            }
            GroupType repeated = group.getType(0).asGroupType();
            Type element = repeated.getType(0);
            GroupType prunedElement = pruneNode(element.asGroupType(), tails);
            return group.withNewFields(repeated.withNewFields(prunedElement));
        }

        Map<String, List<List<Step>>> byKey = new LinkedHashMap<>();
        for (List<Step> suffix : suffixes) {
            if (suffix.get(0).isElement()) {
                return typedValue;
            }
            byKey.computeIfAbsent(suffix.get(0).key().orElseThrow(), _ -> new ArrayList<>()).add(suffix.subList(1, suffix.size()));
        }
        ImmutableList.Builder<Type> kept = ImmutableList.builder();
        boolean any = false;
        for (Type field : group.getFields()) {
            List<List<Step>> tails = byKey.get(field.getName());
            if (tails != null) {
                kept.add(pruneNode(field.asGroupType(), tails));
                any = true;
            }
        }
        if (!any) {
            return witness(group).<Type>map(object -> object).orElse(typedValue);
        }
        return group.withNewFields(kept.build());
    }

    /**
     * Returns the object group reduced to the one primitive column chosen as witness, with the groups leading to it:
     * the first primitive column walking fields in file order, depth first, taking for each field its typed_value if
     * primitive, else its value, else descending into its typed_value.
     */
    static Optional<GroupType> witness(GroupType object)
    {
        for (Type field : object.getFields()) {
            Optional<GroupType> node = witnessNode(field.asGroupType());
            if (node.isPresent()) {
                return Optional.of(object.withNewFields(node.get()));
            }
        }
        return Optional.empty();
    }

    private static Optional<GroupType> witnessNode(GroupType node)
    {
        if (node.containsField(TYPED_VALUE) && node.getType(TYPED_VALUE).isPrimitive()) {
            return Optional.of(node.withNewFields(node.getType(TYPED_VALUE)));
        }
        if (node.containsField(VALUE)) {
            return Optional.of(node.withNewFields(node.getType(VALUE)));
        }
        if (!node.containsField(TYPED_VALUE)) {
            return Optional.empty();
        }
        GroupType group = node.getType(TYPED_VALUE).asGroupType();
        if (isList(group)) {
            GroupType repeated = group.getType(0).asGroupType();
            return witnessNode(repeated.getType(0).asGroupType())
                    .map(element -> node.withNewFields(group.withNewFields(repeated.withNewFields(element))));
        }
        return witness(group).map(object -> node.withNewFields(object));
    }

    // Same distinction as the reader: a LIST-annotated group is a list, any other group an object
    private static boolean isList(GroupType group)
    {
        return group.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation;
    }
}
