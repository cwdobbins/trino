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
import io.trino.parquet.VariantPath;
import io.trino.parquet.reader.VariantPathExtraction.Extraction;
import io.trino.spi.variant.Variant;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.VariantJsonParser;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;

import static io.trino.parquet.VariantPath.Step.element;
import static io.trino.parquet.VariantPath.Step.key;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.createTempDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.deleteDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.prunedSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFilePruned;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readerFileSchema;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.rowGroupCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.shredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.typedValueCount;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.unshredded;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeVariantFile;
import static io.trino.parquet.reader.VariantPathExtraction.describe;
import static io.trino.parquet.reader.VariantPathExtraction.extract;
import static io.trino.parquet.reader.VariantPathExtraction.same;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The pruned variant read's contract, on real files: for every requested path and every row, extracting the path from
 * the variant read through the pruned group gives the same value, the same "missing" and the same shape at every step as
 * extracting it from the variant read whole. Integers in JSON parse as the smallest width, so integer columns are
 * declared int32 (INTEGER(8,true)).
 */
final class TestPrunedVariantRead
{
    // Set to a case seed printed by a failure to run that case alone
    private static final OptionalLong REPLAY_SEED = OptionalLong.empty();
    private static final long RANDOMISED_SEED = 0x5EED_2026_1111L;
    private static final int RANDOMISED_CASES = 200;

    private static final String STRING_FIELD = "{ optional binary value; optional binary typed_value (STRING); }";
    private static final String INT8_FIELD = "{ optional binary value; optional int32 typed_value (INTEGER(8,true)); }";

    // The shared schema of the pruning tests, with integers at the width the JSON parser produces
    static final MessageType SCHEMA = shredded(
            """
            optional group typed_value {
              required group status %1$s
              required group n %2$s
              required group user { optional binary value; optional group typed_value {
                  required group tier %1$s
                  required group geo { optional binary value; optional group typed_value {
                      required group country %1$s
                      required group city %1$s } } } }
              required group items { optional binary value; optional group typed_value (LIST) { repeated group list {
                  required group element { optional binary value; optional group typed_value {
                      required group sku %1$s
                      required group qty %2$s } } } } }
              required group tags { optional binary value; optional group typed_value (LIST) { repeated group list {
                  required group element %1$s } } }
            }
            """.formatted(STRING_FIELD, INT8_FIELD));

    // Each path of the shared schema meets each row shape: shredded, unshredded (another type, in value), missing,
    // variant null, the wrong shape, and data outside the shredded fields
    static final List<String> SHARED_ROWS = ImmutableList.of(
            """
            {"status": "ok", "n": 1, "user": {"tier": "gold", "geo": {"country": "US", "city": "NYC"}},
             "items": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}], "tags": ["x", "y"], "zzz": 5}""",
            """
            {"status": 7, "n": "seven", "user": {"tier": 3, "geo": {"country": 4, "city": true}},
             "items": [{"sku": 9, "qty": "many"}], "tags": [1, null]}""",
            "{}",
            """
            {"user": {}, "items": [{}], "tags": []}""",
            """
            {"status": null, "n": null, "user": null, "items": null, "tags": null}""",
            """
            {"user": {"tier": null, "geo": null}, "items": [null, {"sku": null}], "tags": [null]}""",
            """
            {"status": {"x": 1}, "user": "bob", "items": {"sku": "s"}, "tags": "t", "n": [1]}""",
            """
            {"user": {"geo": [1, 2], "tier": {"a": 1}}, "items": [1, "two", [3]], "tags": [["nested"], {"x": "y"}]}""",
            """
            {"status": "ok", "zzz": {"q": 1}, "user": {"zzz": "z", "geo": {"country": "FR", "zzz": [1]}},
             "items": [{"sku": "c", "zzz": true}, {"zzz": null}]}""",
            """
            {"n": 100000, "status": "s", "items": [{"sku": "a"}, 5, null, "x", [{"sku": "b"}]]}""",
            """
            {"user": {"geo": {"country": {"code": "DE"}, "city": ["Berlin"]}}, "items": [], "tags": ["only"]}""",
            """
            {"user": {"geo": {}, "tier": "t"}}""",
            "\"hello\"",
            "3",
            "[1, {\"status\": \"inner\"}]",
            "null");

    @Test
    void testScalarPaths()
    {
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), ImmutableList.of(
                ImmutableList.of(VariantPath.keys("status")),
                ImmutableList.of(VariantPath.keys("n")),
                ImmutableList.of(VariantPath.keys("status"), VariantPath.keys("n"))));
    }

    @Test
    void testNestedObjectPath()
    {
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), ImmutableList.of(
                ImmutableList.of(VariantPath.keys("user", "geo", "country")),
                ImmutableList.of(VariantPath.keys("user", "tier"))));
    }

    @Test
    void testListMemberPaths()
    {
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), ImmutableList.of(
                ImmutableList.of(VariantPath.of(key("items"), element(), key("sku"))),
                ImmutableList.of(VariantPath.of(key("items"), element(), key("sku")), VariantPath.of(key("items"), element(), key("qty")))));
    }

    @Test
    void testListOfScalarsPath()
    {
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), ImmutableList.of(
                ImmutableList.of(VariantPath.of(key("tags"), element()))));
    }

    @Test
    void testPathNotShreddedInFile()
    {
        // zzz is never a shredded field: its data is in the value of the node above, and the node must still read as an object
        List<String> rows = new ArrayList<>(SHARED_ROWS);
        rows.addAll(ImmutableList.of(
                """
                {"zzz": "top", "user": {"zzz": {"deep": [1, 2]}, "tier": "t"}, "items": [{"zzz": 1, "sku": "k"}, {"zzz": "two"}, {}]}""",
                """
                {"zzz": null, "user": {"zzz": null}, "items": [{"zzz": null}]}""",
                """
                {"status": 1, "user": {"tier": 2}, "items": [{"qty": 300}]}"""));
        rows.add(null);
        assertContract(SCHEMA, rows, ImmutableList.of(
                ImmutableList.of(VariantPath.keys("zzz")),
                ImmutableList.of(VariantPath.keys("user", "zzz")),
                ImmutableList.of(VariantPath.of(key("items"), element(), key("zzz")))));

        // Field groups without a value, so the witness descends: into a list's element, and into an object
        MessageType typedOnly = shredded(
                """
                optional group typed_value {
                  required group l { optional group typed_value (LIST) { repeated group list {
                      required group element { optional binary typed_value (STRING); } } } }
                  required group o { optional group typed_value {
                      required group p { optional int32 typed_value (INTEGER(8,true)); } } }
                  required group s %s
                }
                """.formatted(STRING_FIELD));
        List<String> typedOnlyRows = new ArrayList<>(ImmutableList.of(
                """
                {"l": ["a", "b"], "o": {"p": 1}, "s": "x", "zzz": 1}""",
                """
                {"l": [], "o": {}, "zzz": {"y": 2}}""",
                """
                {"s": 5, "zzz": [3]}""",
                """
                {"l": ["only"], "q": "residual"}""",
                "{}",
                "\"not an object\"",
                "[1, 2]",
                "null"));
        typedOnlyRows.add(null);
        assertContract(typedOnly, typedOnlyRows, ImmutableList.of(
                ImmutableList.of(VariantPath.keys("zzz")),
                ImmutableList.of(VariantPath.keys("zzz", "y")),
                ImmutableList.of(VariantPath.keys("o", "zzz")),
                ImmutableList.of(VariantPath.keys("q")),
                ImmutableList.of(VariantPath.of(key("l"), element(), key("x"))),
                ImmutableList.of(VariantPath.of(key("l"), element()))));
    }

    @Test
    void testShapeMismatchRows()
    {
        List<String> rows = ImmutableList.of(
                """
                {"status": {"x": "object status"}, "user": "a string user", "items": {"sku": "items as object"}, "tags": {"0": "x"}}""",
                """
                {"status": ["list status"], "user": ["list user"], "items": [7, null, true, {"sku": "real"}], "tags": [1, null, "s", {}]}""",
                """
                {"user": {"geo": "a string geo", "tier": ["list tier"]}, "items": [[1], {"sku": {"nested": "sku"}}]}""",
                """
                {"user": {"geo": {"country": [1], "city": {"name": "x"}}}}""",
                "\"a string document\"",
                "12",
                "true",
                "[{\"status\": \"in a list\"}]",
                "null",
                """
                {"status": "ok", "user": {"tier": "gold", "geo": {"country": "US"}}, "items": [{"sku": "a", "qty": 3}], "tags": ["x"]}""");
        assertContract(SCHEMA, rowsWithSqlNull(rows), ImmutableList.of(
                ImmutableList.of(VariantPath.keys("status")),
                ImmutableList.of(VariantPath.keys("user", "tier")),
                ImmutableList.of(VariantPath.keys("user", "geo", "country")),
                ImmutableList.of(VariantPath.keys("user", "geo", "city")),
                ImmutableList.of(VariantPath.of(key("items"), element(), key("sku"))),
                ImmutableList.of(VariantPath.of(key("tags"), element())),
                ImmutableList.of(VariantPath.of(key("tags"), element()), VariantPath.of(key("items"), element(), key("qty")), VariantPath.keys("status"))));
    }

    @Test
    void testLayoutMismatchPaths()
    {
        // a key step on a shredded list, an element step on a shredded object, a key step on a shredded scalar
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), ImmutableList.of(
                ImmutableList.of(VariantPath.keys("items", "sku")),
                ImmutableList.of(VariantPath.of(key("user"), element())),
                ImmutableList.of(VariantPath.keys("status", "x")),
                ImmutableList.of(VariantPath.of(key("tags"), element(), key("x"))),
                ImmutableList.of(VariantPath.of(key("items"), element(), element())),
                ImmutableList.of(VariantPath.keys("user", "geo", "country", "code"))));
    }

    @Test
    void testAwkwardKeys()
    {
        List<String> names = ImmutableList.of("", "a.b", "A", "a", "value", "typed_value", "metadata");
        Types.GroupBuilder<GroupType> object = Types.buildGroup(OPTIONAL);
        for (String name : names) {
            object.addField(Types.buildGroup(REQUIRED)
                    .addField(Types.optional(BINARY).named("value"))
                    .addField(Types.optional(BINARY).as(stringType()).named("typed_value"))
                    .named(name));
        }
        MessageType schema = new MessageType("m", Types.buildGroup(OPTIONAL)
                .as(LogicalTypeAnnotation.variantType((byte) 1))
                .addField(Types.required(BINARY).named("metadata"))
                .addField(Types.optional(BINARY).named("value"))
                .addField(object.named("typed_value"))
                .named("v"));
        List<String> rows = ImmutableList.of(
                """
                {"": "empty", "a.b": "dotted", "A": "upper", "a": "lower", "value": "v", "typed_value": "t", "metadata": "m", "b": "residual"}""",
                """
                {"a": {"b": "a then b"}, "A": 1, "": 2, "value": {"x": 1}, "typed_value": [1], "metadata": null}""",
                """
                {"a.b": {"c": 3}, "b": {"a": "x"}}""",
                "{}",
                "\"scalar\"",
                """
                {"a": "only a"}""",
                """
                {"a": null, "A": "x"}""",
                """
                {"a": [1, {"b": "in a list"}]}""",
                """
                {"a": {}}""");
        List<List<VariantPath>> pathSets = new ArrayList<>();
        for (String name : names) {
            pathSets.add(ImmutableList.of(VariantPath.keys(name)));
        }
        pathSets.add(ImmutableList.of(VariantPath.keys("a", "b")));
        pathSets.add(ImmutableList.of(VariantPath.keys("a.b", "c")));
        pathSets.add(ImmutableList.of(VariantPath.keys("b")));
        pathSets.add(ImmutableList.of(VariantPath.keys("b", "a")));
        pathSets.add(ImmutableList.of(VariantPath.keys("A"), VariantPath.keys("a"), VariantPath.keys("a", "b")));
        pathSets.add(ImmutableList.of(VariantPath.keys("value", "x"), VariantPath.of(key("typed_value"), element()), VariantPath.keys("metadata")));
        assertContract(schema, rowsWithSqlNull(rows), pathSets);
    }

    @Test
    void testListOfLists()
    {
        MessageType schema = shredded(
                """
                optional group typed_value {
                  required group m { optional binary value; optional group typed_value (LIST) { repeated group list {
                      required group element { optional binary value; optional group typed_value (LIST) { repeated group list {
                          required group element %s } } } } } }
                  required group k %s
                }
                """.formatted(INT8_FIELD, STRING_FIELD));
        List<String> rows = ImmutableList.of(
                """
                {"m": [[1, 2], [3]], "k": "a"}""",
                """
                {"m": [[], [null], null], "k": "b"}""",
                """
                {"m": [1, [2, "two", 1000]], "k": 3}""",
                """
                {"m": {"a": [1]}}""",
                """
                {"m": [[{"x": 1}], [[4]]]}""",
                """
                {"m": []}""",
                """
                {"m": "flat"}""",
                "{}",
                "[[1]]",
                "null",
                """
                {"m": null, "k": "n"}""");
        assertContract(schema, rowsWithSqlNull(rows), ImmutableList.of(
                ImmutableList.of(VariantPath.of(key("m"), element(), element())),
                ImmutableList.of(VariantPath.of(key("m"), element())),
                ImmutableList.of(VariantPath.of(key("m"), element(), element(), key("x"))),
                ImmutableList.of(VariantPath.of(key("m"), element(), key("a")))));
    }

    @Test
    void testManyAndOverlappingPaths()
    {
        // Every path but those through n, which then must not be read; duplicates and covered pairs included
        List<VariantPath> paths = ImmutableList.of(
                VariantPath.keys("status"),
                VariantPath.keys("status"),
                VariantPath.keys("status", "x"),
                VariantPath.of(key("status"), element()),
                VariantPath.keys("user"),
                VariantPath.keys("user", "tier"),
                VariantPath.keys("user", "tier"),
                VariantPath.keys("user", "geo"),
                VariantPath.keys("user", "geo", "country"),
                VariantPath.keys("user", "geo", "city"),
                VariantPath.keys("user", "geo", "zzz"),
                VariantPath.keys("user", "zzz"),
                VariantPath.of(key("user"), element()),
                VariantPath.of(key("user"), key("geo"), element()),
                VariantPath.keys("items"),
                VariantPath.keys("items", "sku"),
                VariantPath.of(key("items"), element()),
                VariantPath.of(key("items"), element(), key("sku")),
                VariantPath.of(key("items"), element(), key("sku")),
                VariantPath.of(key("items"), element(), key("qty")),
                VariantPath.of(key("items"), element(), key("zzz")),
                VariantPath.of(key("items"), element(), element()),
                VariantPath.of(key("items"), element(), key("sku"), key("x")),
                VariantPath.keys("tags"),
                VariantPath.of(key("tags"), element()),
                VariantPath.of(key("tags"), element()),
                VariantPath.of(key("tags"), element(), key("x")),
                VariantPath.of(key("tags"), element(), element()),
                VariantPath.keys("tags", "x"),
                VariantPath.keys("zzz"),
                VariantPath.keys("zzz"),
                VariantPath.keys("zzz", "q"),
                VariantPath.keys("zzz", "q", "r"),
                VariantPath.keys("other"),
                VariantPath.keys(""),
                VariantPath.keys("STATUS"),
                VariantPath.keys("user", "TIER"),
                VariantPath.keys("user", "geo", "country", "code"),
                VariantPath.of(key("items"), element(), key("sku"), element()),
                VariantPath.keys("user", "geo", "city"));
        assertThat(paths).hasSize(40);
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), ImmutableList.of(paths));
        // A few paths at a time, overlapping
        List<List<VariantPath>> windows = new ArrayList<>();
        for (int start = 0; start + 3 <= paths.size(); start += 2) {
            windows.add(paths.subList(start, start + 3));
        }
        assertContract(SCHEMA, rowsWithSqlNull(SHARED_ROWS), windows);
    }

    @Test
    void testUnshreddedFile()
    {
        List<String> rows = rowsWithSqlNull(SHARED_ROWS);
        Path directory = createTempDirectory();
        try {
            File file = writeFile(directory, "plain.parquet", unshredded(), rows);
            assertThat(typedValueCount(file)).isEqualTo(0);
            List<Variant> whole = readFile(file);
            for (List<VariantPath> paths : ImmutableList.of(
                    ImmutableList.of(VariantPath.keys("status")),
                    ImmutableList.of(VariantPath.of(key("items"), element(), key("sku")), VariantPath.keys("user", "geo")),
                    ImmutableList.of(VariantPath.keys("zzz", "q"), VariantPath.of(key("tags"), element())))) {
                MessageType fileSchema = readerFileSchema(file);
                assertThat(prunedSchema(fileSchema, paths)).isEqualTo(fileSchema);
                List<Variant> pruned = readFilePruned(file, paths);
                assertLogicallyEqual(pruned, whole, rows);
                assertSamePaths(whole, pruned, paths, rows);
            }
        }
        finally {
            deleteDirectory(directory);
        }
    }

    @Test
    void testAcrossRowGroupsAndBatches()
    {
        List<String> rows = new ArrayList<>(30_000);
        for (int i = 0; i < 30_000; i++) {
            if (i % 7 == 0) {
                rows.add(i % 91 == 0 ? null : SHARED_ROWS.get((i / 7) % SHARED_ROWS.size()));
                continue;
            }
            StringBuilder items = new StringBuilder();
            for (int j = 0; j < i % 4; j++) {
                items.append(j == 0 ? "" : ", ").append("{\"sku\": \"sku-%s-%s\", \"qty\": %s}".formatted(i, j, j));
            }
            rows.add(
                    """
                    {"status": "status-%s", "n": %s, "user": {"tier": "tier-%s", "geo": {"country": "c%s"}}, "items": [%s], "tags": ["t%s"]%s}"""
                            .formatted(i, i % 100, i % 5, i % 13, items, i, i % 3 == 0 ? ", \"zzz\": " + i : ""));
        }
        List<org.apache.parquet.variant.Variant> variants = new ArrayList<>(rows.size());
        for (String row : rows) {
            try {
                variants.add(row == null ? null : VariantJsonParser.parseJson(row));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        List<List<VariantPath>> pathSets = ImmutableList.of(
                ImmutableList.of(VariantPath.keys("status"), VariantPath.of(key("items"), element(), key("sku"))),
                ImmutableList.of(VariantPath.keys("zzz"), VariantPath.keys("user", "geo", "country")));
        Path directory = createTempDirectory();
        try {
            File file = writeVariantFile(directory, "file.parquet", SCHEMA, variants, 128 * 1024, 4 * 1024);
            assertThat(rowGroupCount(file)).isGreaterThan(1);
            assertThat(typedValueCount(file)).isGreaterThan(100_000);
            List<Variant> whole = readFile(file);
            assertLogicallyEqual(whole, readFile(writeVariantFile(directory, "plain.parquet", unshredded(), variants)), rows);
            for (List<VariantPath> paths : pathSets) {
                assertFewerLeaves(file, paths);
                for (int batchSize : new int[] {1, 97, 1024, 8192}) {
                    List<Variant> pruned = readFilePruned(file, paths, batchSize);
                    assertSamePaths(whole, pruned, paths, rows);
                }
            }
        }
        finally {
            deleteDirectory(directory);
        }
    }

    @Test
    void testRandomised()
    {
        Path directory = createTempDirectory();
        try {
            if (REPLAY_SEED.isPresent()) {
                runRandomCase(directory, -1, REPLAY_SEED.orElseThrow());
                return;
            }
            Random seeds = new Random(RANDOMISED_SEED);
            for (int i = 0; i < RANDOMISED_CASES; i++) {
                runRandomCase(directory, i, seeds.nextLong());
            }
        }
        finally {
            deleteDirectory(directory);
        }
    }

    private static void runRandomCase(Path directory, int index, long seed)
    {
        RandomCase randomCase = RandomCase.generate(new Random(seed));
        try {
            File file = writeFile(directory, "case-" + index + ".parquet", randomCase.schema(), randomCase.rows());
            assertThat(typedValueCount(file)).as("typed values written").isGreaterThan(0);
            List<Variant> whole = readFile(file);
            File plain = writeFile(directory, "case-" + index + "-plain.parquet", unshredded(), randomCase.rows());
            assertLogicallyEqual(whole, readFile(plain), randomCase.rows());
            MessageType fileSchema = readerFileSchema(file);
            assertThat(prunedSchema(fileSchema, randomCase.paths()).getColumns().size()).isLessThanOrEqualTo(fileSchema.getColumns().size());
            assertSamePaths(whole, readFilePruned(file, randomCase.paths()), randomCase.paths(), randomCase.rows());
        }
        catch (RuntimeException | AssertionError e) {
            throw new AssertionError("randomised case %s failed; to replay it alone set REPLAY_SEED = OptionalLong.of(%sL)%nschema: %s%npaths: %s"
                    .formatted(index, seed, randomCase.schema(), randomCase.paths()), e);
        }
    }

    private static List<String> rowsWithSqlNull(List<String> rows)
    {
        List<String> withNull = new ArrayList<>(rows);
        withNull.add(null);
        return withNull;
    }

    /**
     * Writes the rows in the shredded layout (asserting typed values were written), checks the whole read against the
     * unshredded layout, then for each path set checks that the pruned requested schema reads strictly fewer leaf
     * columns than the file has, and the contract.
     */
    private static void assertContract(MessageType schema, List<String> rows, List<List<VariantPath>> pathSets)
    {
        Path directory = createTempDirectory();
        try {
            File file = writeFile(directory, "shredded.parquet", schema, rows);
            assertThat(typedValueCount(file)).as("typed values written").isGreaterThan(0);
            List<Variant> whole = readFile(file);
            assertLogicallyEqual(whole, readFile(writeFile(directory, "plain.parquet", unshredded(), rows)), rows);
            for (List<VariantPath> paths : pathSets) {
                assertFewerLeaves(file, paths);
                assertSamePaths(whole, readFilePruned(file, paths), paths, rows);
            }
        }
        finally {
            deleteDirectory(directory);
        }
    }

    private static void assertFewerLeaves(File file, List<VariantPath> paths)
    {
        MessageType fileSchema = readerFileSchema(file);
        assertThat(prunedSchema(fileSchema, paths).getColumns().size())
                .as("leaf columns read for %s", paths)
                .isLessThan(fileSchema.getColumns().size());
    }

    private static void assertSamePaths(List<Variant> whole, List<Variant> pruned, List<VariantPath> paths, List<String> rows)
    {
        assertThat(pruned).as("rows read pruned for %s", paths).hasSameSizeAs(whole);
        for (VariantPath path : paths) {
            for (int row = 0; row < whole.size(); row++) {
                Extraction expected = extract(whole.get(row), path);
                Extraction actual = extract(pruned.get(row), path);
                if (!same(actual, expected)) {
                    fail("path %s of row %s (%s), reading %s: pruned read gives %s, whole read gives %s".formatted(
                            path, row, rows.get(row), paths, describe(actual), describe(expected)));
                }
            }
        }
    }

    /**
     * A generated case: documents that mostly follow one random shape, a shredding schema for a random part of that
     * shape, and a few paths, some following the shape, some absent from it and some of the wrong shape.
     */
    private record RandomCase(MessageType schema, List<String> rows, List<VariantPath> paths)
    {
        private static final List<String> KEYS = ImmutableList.of("a", "b", "c", "d");
        private static final List<String> STRINGS = ImmutableList.of("x", "yy", "", "a longer string value", "Ünïcode");
        private static final int ROWS = 30;
        private static final int MAX_DEPTH = 4;

        private sealed interface Shape {}

        private record ObjectShape(Map<String, Shape> fields)
                implements Shape {}

        private record ListShape(Shape element)
                implements Shape {}

        private enum Scalar
        {
            STRING, INT8, BOOLEAN
        }

        private record ScalarShape(Scalar kind)
                implements Shape {}

        static RandomCase generate(Random random)
        {
            Shape shape = objectShape(random, 0);
            String typedValue;
            do {
                typedValue = typedValue(random, shape);
            }
            while (!typedValue.contains("typed_value (STRING)") && !typedValue.contains("typed_value (INTEGER") && !typedValue.contains("boolean typed_value"));
            MessageType schema = shredded(typedValue);

            List<String> rows = new ArrayList<>();
            for (int i = 0; i < ROWS; i++) {
                int roll = random.nextInt(100);
                if (roll < 4) {
                    rows.add(null);
                }
                else if (roll < 10) {
                    rows.add(noise(random, 1));
                }
                else {
                    rows.add(document(random, shape, 0));
                }
            }

            List<VariantPath> paths = new ArrayList<>();
            int pathCount = 1 + random.nextInt(5);
            for (int i = 0; i < pathCount; i++) {
                paths.add(path(random, shape));
            }
            return new RandomCase(schema, rows, paths);
        }

        private static Shape shape(Random random, int depth)
        {
            int roll = random.nextInt(100);
            if (depth >= MAX_DEPTH - 1 || roll < 40) {
                return new ScalarShape(Scalar.values()[random.nextInt(Scalar.values().length)]);
            }
            if (roll < 75) {
                return objectShape(random, depth);
            }
            return new ListShape(shape(random, depth + 1));
        }

        private static ObjectShape objectShape(Random random, int depth)
        {
            Map<String, Shape> fields = new LinkedHashMap<>();
            int count = 1 + random.nextInt(KEYS.size());
            List<String> keys = new ArrayList<>(KEYS);
            Collections.shuffle(keys, random);
            for (String key : keys.subList(0, count)) {
                fields.put(key, shape(random, depth + 1));
            }
            return new ObjectShape(fields);
        }

        // The typed_value of a node of this shape, shredding a random part of it
        private static String typedValue(Random random, Shape shape)
        {
            return switch (shape) {
                case ScalarShape(Scalar kind) -> switch (kind) {
                    case STRING -> "optional binary typed_value (STRING);";
                    case INT8 -> "optional int32 typed_value (INTEGER(8,true));";
                    case BOOLEAN -> "optional boolean typed_value;";
                };
                case ListShape(Shape element) -> "optional group typed_value (LIST) { repeated group list { required group element %s } }".formatted(node(random, element));
                case ObjectShape(Map<String, Shape> fields) -> {
                    List<String> chosen = new ArrayList<>();
                    for (Map.Entry<String, Shape> field : fields.entrySet()) {
                        if (random.nextInt(100) < 70) {
                            chosen.add("required group %s %s".formatted(field.getKey(), node(random, field.getValue())));
                        }
                    }
                    if (chosen.isEmpty()) {
                        Map.Entry<String, Shape> field = fields.entrySet().iterator().next();
                        chosen.add("required group %s %s".formatted(field.getKey(), node(random, field.getValue())));
                    }
                    yield "optional group typed_value { %s }".formatted(String.join(" ", chosen));
                }
            };
        }

        // A node: always a value, and a typed_value three times in four
        private static String node(Random random, Shape shape)
        {
            if (random.nextInt(100) < 25) {
                return "{ optional binary value; }";
            }
            return "{ optional binary value; %s }".formatted(typedValue(random, shape));
        }

        private static String document(Random random, Shape shape, int depth)
        {
            if (depth > 0 && random.nextInt(100) < 12) {
                return noise(random, depth);
            }
            return switch (shape) {
                case ScalarShape(Scalar kind) -> scalar(random, kind);
                case ListShape(Shape element) -> {
                    List<String> elements = new ArrayList<>();
                    int length = random.nextInt(4);
                    for (int i = 0; i < length; i++) {
                        elements.add(document(random, element, depth + 1));
                    }
                    yield "[" + String.join(", ", elements) + "]";
                }
                case ObjectShape(Map<String, Shape> fields) -> {
                    List<String> members = new ArrayList<>();
                    for (Map.Entry<String, Shape> field : fields.entrySet()) {
                        if (random.nextInt(100) >= 15) {
                            members.add("\"%s\": %s".formatted(field.getKey(), document(random, field.getValue(), depth + 1)));
                        }
                    }
                    if (random.nextInt(100) < 20) {
                        members.add("\"r\": " + noise(random, depth + 1));
                    }
                    yield "{" + String.join(", ", members) + "}";
                }
            };
        }

        private static String scalar(Random random, Scalar kind)
        {
            return switch (kind) {
                case STRING -> "\"" + STRINGS.get(random.nextInt(STRINGS.size())) + "\"";
                case INT8 -> Integer.toString(random.nextInt(201) - 100);
                case BOOLEAN -> Boolean.toString(random.nextBoolean());
            };
        }

        // A value of any shape, ignoring the case's shape
        private static String noise(Random random, int depth)
        {
            int roll = random.nextInt(100);
            if (depth >= MAX_DEPTH) {
                roll = roll % 60;
            }
            if (roll < 15) {
                return "null";
            }
            if (roll < 25) {
                return scalar(random, Scalar.STRING);
            }
            if (roll < 35) {
                return scalar(random, Scalar.INT8);
            }
            if (roll < 45) {
                return Integer.toString(1000 + random.nextInt(1000));
            }
            if (roll < 55) {
                return scalar(random, Scalar.BOOLEAN);
            }
            if (roll < 60) {
                return random.nextBoolean() ? "{}" : "[]";
            }
            if (roll < 80) {
                List<String> members = new ArrayList<>();
                for (String key : KEYS) {
                    if (random.nextBoolean()) {
                        members.add("\"%s\": %s".formatted(key, noise(random, depth + 1)));
                    }
                }
                return "{" + String.join(", ", members) + "}";
            }
            List<String> elements = new ArrayList<>();
            int length = 1 + random.nextInt(3);
            for (int i = 0; i < length; i++) {
                elements.add(noise(random, depth + 1));
            }
            return "[" + String.join(", ", elements) + "]";
        }

        // A path that mostly follows the shape, sometimes leaves it (an absent key) or meets the wrong shape
        private static VariantPath path(Random random, Shape shape)
        {
            if (random.nextInt(100) < 3) {
                return VariantPath.of();
            }
            List<VariantPath.Step> steps = new ArrayList<>();
            Shape current = shape;
            int length = 1 + random.nextInt(5);
            while (steps.size() < length) {
                int roll = random.nextInt(100);
                switch (current) {
                    case ObjectShape(Map<String, Shape> fields) -> {
                        if (roll < 70) {
                            List<String> keys = new ArrayList<>(fields.keySet());
                            String key = keys.get(random.nextInt(keys.size()));
                            steps.add(key(key));
                            current = fields.get(key);
                        }
                        else if (roll < 80) {
                            steps.add(key("zz"));
                            current = null;
                        }
                        else if (roll < 90) {
                            steps.add(key("r"));
                            current = null;
                        }
                        else {
                            steps.add(element());
                            current = null;
                        }
                    }
                    case ListShape(Shape element) -> {
                        if (roll < 80) {
                            steps.add(element());
                            current = element;
                        }
                        else {
                            steps.add(key(KEYS.get(random.nextInt(KEYS.size()))));
                            current = null;
                        }
                    }
                    case ScalarShape _ -> {
                        steps.add(roll < 50 ? key(KEYS.get(random.nextInt(KEYS.size()))) : element());
                        current = null;
                    }
                    case null -> steps.add(roll < 70 ? key(KEYS.get(random.nextInt(KEYS.size()))) : element());
                }
            }
            return new VariantPath(steps);
        }
    }
}
