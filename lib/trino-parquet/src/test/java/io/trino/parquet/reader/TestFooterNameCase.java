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
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static io.trino.parquet.reader.ShreddedVariantTestUtils.createTempDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.deleteDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeGroupFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the footer names columns: lowercased, as it always has, except below a VARIANT group.
 */
final class TestFooterNameCase
{
    @Test
    void testCollidingNamesInFileWithoutVariantParseAsBefore()
            throws IOException
    {
        // a group a and a sibling primitive A collide once lowercased; the footer must still parse and name columns as before
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group a {
                    optional int32 x;
                  }
                  optional int32 A;
                }
                """);
        SimpleGroup row = new SimpleGroup(schema);
        row.addGroup("a").append("x", 1);
        row.append("A", 2);
        // Stock 484 (checked by running this test against its ParquetMetadata) parses the footer and then fails in
        // getBlocks with parquet-java's InvalidRecordException, because the lowercased schema has two fields named a.
        assertThatThrownBy(() -> columnPaths(schema, ImmutableList.of(row)))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("path invalid: [a, x]");
    }

    @Test
    void testVariantChildrenKeepCaseAndOtherColumnsAreLowercased()
            throws IOException
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group Mixed {
                    optional int32 Inner;
                  }
                  optional group v (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group eventType { optional binary value; optional binary typed_value (STRING); }
                      required group A { optional binary value; optional int32 typed_value (INTEGER(8,true)); }
                    }
                  }
                }
                """);
        SimpleGroup row = new SimpleGroup(schema);
        row.addGroup("Mixed").append("Inner", 1);
        row.addGroup("v").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0}));
        assertThat(columnPaths(schema, ImmutableList.of(row))).containsExactly(
                ImmutableList.of("mixed", "inner"),
                ImmutableList.of("v", "metadata"),
                ImmutableList.of("v", "value"),
                ImmutableList.of("v", "typed_value", "eventType", "value"),
                ImmutableList.of("v", "typed_value", "eventType", "typed_value"),
                ImmutableList.of("v", "typed_value", "A", "value"),
                ImmutableList.of("v", "typed_value", "A", "typed_value"));
    }

    @Test
    void testVariantBelowMixedCaseAncestorKeepsCaseOnlyBelowTheVariant()
            throws IOException
    {
        // the struct and the variant group are lowercased like any column; only the names below the variant keep their case
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group Outer {
                    optional int32 Id;
                    optional group V (VARIANT(1)) {
                      required binary metadata;
                      optional binary value;
                      optional group typed_value {
                        required group Key { optional binary value; optional binary typed_value (STRING); }
                      }
                    }
                  }
                }
                """);
        SimpleGroup row = new SimpleGroup(schema);
        Group outer = row.addGroup("Outer");
        outer.append("Id", 1);
        outer.addGroup("V").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0}));
        assertThat(columnPaths(schema, ImmutableList.of(row))).containsExactly(
                ImmutableList.of("outer", "id"),
                ImmutableList.of("outer", "v", "metadata"),
                ImmutableList.of("outer", "v", "value"),
                ImmutableList.of("outer", "v", "typed_value", "Key", "value"),
                ImmutableList.of("outer", "v", "typed_value", "Key", "typed_value"));
    }

    @Test
    void testTwoVariantGroupsEachKeepCaseBelowThemselves()
            throws IOException
    {
        // two variant groups, one at the top and one in a struct, each with mixed-case keys, beside mixed-case plain columns
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message m {
                  optional group First (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group camelKey { optional binary value; optional binary typed_value (STRING); }
                    }
                  }
                  optional int32 Plain;
                  optional group S {
                    optional group Second (VARIANT(1)) {
                      required binary metadata;
                      optional binary value;
                      optional group typed_value {
                        required group UPPER { optional binary value; optional int32 typed_value (INTEGER(8,true)); }
                        required group lower { optional binary value; optional binary typed_value (STRING); }
                      }
                    }
                  }
                }
                """);
        SimpleGroup row = new SimpleGroup(schema);
        row.addGroup("First").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0}));
        row.append("Plain", 2);
        row.addGroup("S").addGroup("Second").append("metadata", Binary.fromConstantByteArray(new byte[] {1, 0}));
        assertThat(columnPaths(schema, ImmutableList.of(row))).containsExactly(
                ImmutableList.of("first", "metadata"),
                ImmutableList.of("first", "value"),
                ImmutableList.of("first", "typed_value", "camelKey", "value"),
                ImmutableList.of("first", "typed_value", "camelKey", "typed_value"),
                ImmutableList.of("plain"),
                ImmutableList.of("s", "second", "metadata"),
                ImmutableList.of("s", "second", "value"),
                ImmutableList.of("s", "second", "typed_value", "UPPER", "value"),
                ImmutableList.of("s", "second", "typed_value", "UPPER", "typed_value"),
                ImmutableList.of("s", "second", "typed_value", "lower", "value"),
                ImmutableList.of("s", "second", "typed_value", "lower", "typed_value"));
    }

    private static List<List<String>> columnPaths(MessageType schema, List<Group> rows)
            throws IOException
    {
        Path directory = createTempDirectory();
        try {
            File file = writeGroupFile(directory, "case.parquet", schema, rows);
            TestingParquetDataSource dataSource = new TestingParquetDataSource(Slices.wrappedBuffer(Files.readAllBytes(file.toPath())), ParquetReaderOptions.defaultOptions());
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
            assertThat(metadata.getFileMetaData().getSchema()).isNotNull();
            ImmutableList.Builder<List<String>> paths = ImmutableList.builder();
            for (BlockMetadata block : metadata.getBlocks()) {
                for (ColumnChunkMetadata column : block.columns()) {
                    paths.add(ImmutableList.copyOf(column.getPath().toArray()));
                }
            }
            return paths.build();
        }
        finally {
            deleteDirectory(directory);
        }
    }
}
