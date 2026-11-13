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
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * A path into a VARIANT value: object keys and "every element of a list" steps.
 * Keys are exact strings; names such as "value" or "typed_value" are ordinary keys.
 */
public record VariantPath(List<Step> steps)
{
    /**
     * One step of a path. An empty key means "every element of a list".
     */
    public record Step(Optional<String> key)
    {
        public Step
        {
            requireNonNull(key, "key is null");
        }

        public static Step key(String key)
        {
            return new Step(Optional.of(requireNonNull(key, "key is null")));
        }

        public static Step element()
        {
            return new Step(Optional.empty());
        }

        public boolean isElement()
        {
            return key.isEmpty();
        }
    }

    public VariantPath
    {
        steps = ImmutableList.copyOf(requireNonNull(steps, "steps is null"));
    }

    public static VariantPath of(Step... steps)
    {
        return new VariantPath(ImmutableList.copyOf(steps));
    }

    public static VariantPath keys(String... keys)
    {
        ImmutableList.Builder<Step> steps = ImmutableList.builder();
        for (String key : keys) {
            steps.add(Step.key(key));
        }
        return new VariantPath(steps.build());
    }

    public VariantPath append(Step step)
    {
        return new VariantPath(ImmutableList.<Step>builder().addAll(steps).add(requireNonNull(step, "step is null")).build());
    }

    /**
     * Returns true if this path is a prefix of the other; a path is a prefix of itself.
     */
    public boolean isPrefixOf(VariantPath other)
    {
        return steps.size() <= other.steps.size() && other.steps.subList(0, steps.size()).equals(steps);
    }

    /**
     * Removes duplicates and every path that has another path in the collection as a prefix.
     * The result is unmodifiable and sorted by {@link #toString()}, so iteration order is deterministic.
     */
    public static Set<VariantPath> normalize(Collection<VariantPath> paths)
    {
        List<VariantPath> distinct = paths.stream()
                .distinct()
                .sorted(Comparator.comparing(VariantPath::toString))
                .collect(toImmutableList());
        ImmutableSet.Builder<VariantPath> result = ImmutableSet.builder();
        for (VariantPath path : distinct) {
            boolean covered = false;
            for (VariantPath other : distinct) {
                if (!other.equals(path) && other.isPrefixOf(path)) {
                    covered = true;
                    break;
                }
            }
            if (!covered) {
                result.add(path);
            }
        }
        return result.build();
    }

    /**
     * For error messages only; nothing parses it back.
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("$");
        for (Step step : steps) {
            if (step.isElement()) {
                builder.append("[*]");
            }
            else {
                builder.append("['")
                        .append(step.key().orElseThrow().replace("\\", "\\\\").replace("'", "\\'"))
                        .append("']");
            }
        }
        return builder.toString();
    }
}
