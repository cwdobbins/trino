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
import io.trino.spi.variant.Variant;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.assertLogicallyEqual;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.createTempDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.deleteDirectory;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.readFile;
import static io.trino.parquet.reader.ShreddedVariantTestUtils.writeGroupFile;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;

/**
 * One test per example table in the Parquet VariantShredding spec. Every file is built by hand, group by
 * group, because several rows describe states a writer would not emit. Expected values are literals from the
 * first column of each table.
 */
final class TestShreddedVariantSpecTables
{
    private static final long EVENT_TS_1 = 1729794114937L;
    private static final long EVENT_TS_2 = 1729794146402L;
    private static final long EVENT_TS_3 = 1729794240241L;
    private static final long EVENT_TS_4 = 1729794954163L;
    private static final long EVENT_TS_5 = 1729795057774L;

    private static final MessageType PRIMITIVE_SCHEMA = MessageTypeParser.parseMessageType(
            """
            message m {
              optional group v (VARIANT(1)) {
                required binary metadata;
                optional binary value;
                optional int32 typed_value;
              }
            }
            """);

    private static final MessageType ARRAY_SCHEMA = MessageTypeParser.parseMessageType(
            """
            message m {
              optional group v (VARIANT(1)) {
                required binary metadata;
                optional binary value;
                optional group typed_value (LIST) {
                  repeated group list {
                    required group element {
                      optional binary value;
                      optional binary typed_value (STRING);
                    }
                  }
                }
              }
            }
            """);

    private static final MessageType OBJECT_SCHEMA = MessageTypeParser.parseMessageType(
            """
            message m {
              optional group v (VARIANT(1)) {
                required binary metadata;
                optional binary value;
                optional group typed_value {
                  required group event_type {
                    optional binary value;
                    optional binary typed_value (STRING);
                  }
                  required group event_ts {
                    optional binary value;
                    optional int64 typed_value (TIMESTAMP(MICROS,true));
                  }
                }
              }
            }
            """);

    @Test
    void testPrimitiveTable()
    {
        List<Group> rows = new ArrayList<>();
        // 34: value null, typed_value 34
        rows.add(variantRow(PRIMITIVE_SCHEMA, EMPTY_METADATA.toSlice().getBytes(), null, group -> group.append("typed_value", 34)));
        // null: value 00 (Variant null), typed_value null
        rows.add(variantRow(PRIMITIVE_SCHEMA, EMPTY_METADATA.toSlice().getBytes(), Variant.NULL_VALUE.data().getBytes(), _ -> {}));
        // "n/a": value holds the Variant string, typed_value null. The spec prints the bytes as `13 6E 2F 61`;
        // the short string header for three characters is 0D, which Variant.ofString writes.
        rows.add(variantRow(PRIMITIVE_SCHEMA, EMPTY_METADATA.toSlice().getBytes(), Variant.ofString("n/a").data().getBytes(), _ -> {}));
        // 100: value null, typed_value 100
        rows.add(variantRow(PRIMITIVE_SCHEMA, EMPTY_METADATA.toSlice().getBytes(), null, group -> group.append("typed_value", 100)));

        assertTable(
                PRIMITIVE_SCHEMA,
                rows,
                ImmutableList.of(
                        Variant.ofInt(34),
                        Variant.NULL_VALUE,
                        Variant.ofString("n/a"),
                        Variant.ofInt(100)),
                ImmutableList.of("34", "null", "\"n/a\"", "100"));
    }

    @Test
    void testArrayTable()
    {
        List<Group> rows = new ArrayList<>();
        // ["comedy", "drama"]
        rows.add(arrayRow(null, element(null, "comedy"), element(null, "drama")));
        // ["horror", null]: the null element is a Variant null in value
        rows.add(arrayRow(null, element(null, "horror"), element(Variant.NULL_VALUE.data().getBytes(), null)));
        // ["comedy", "drama", "romance"]
        rows.add(arrayRow(null, element(null, "comedy"), element(null, "drama"), element(null, "romance")));
        // null: value 00, typed_value null
        SimpleGroup nullRow = new SimpleGroup(ARRAY_SCHEMA);
        Group v = nullRow.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes()));
        v.append("value", Binary.fromConstantByteArray(Variant.NULL_VALUE.data().getBytes()));
        rows.add(nullRow);

        assertTable(
                ARRAY_SCHEMA,
                rows,
                ImmutableList.of(
                        Variant.ofArray(ImmutableList.of(Variant.ofString("comedy"), Variant.ofString("drama"))),
                        Variant.ofArray(ImmutableList.of(Variant.ofString("horror"), Variant.NULL_VALUE)),
                        Variant.ofArray(ImmutableList.of(Variant.ofString("comedy"), Variant.ofString("drama"), Variant.ofString("romance"))),
                        Variant.NULL_VALUE),
                ImmutableList.of(
                        "[\"comedy\", \"drama\"]",
                        "[\"horror\", null]",
                        "[\"comedy\", \"drama\", \"romance\"]",
                        "null"));
    }

    @Test
    void testObjectTable()
    {
        List<Group> rows = new ArrayList<>();
        List<Variant> expected = new ArrayList<>();
        List<String> descriptions = new ArrayList<>();

        // {"event_type": "noop", "event_ts": 1729794114937}: fully shredded
        rows.add(objectRow(null, group -> {
            field(group, "event_type").append("typed_value", Binary.fromString("noop"));
            field(group, "event_ts").append("typed_value", EVENT_TS_1);
        }));
        expected.add(event(Variant.ofString("noop"), Variant.ofTimestampMicrosUtc(EVENT_TS_1)));
        descriptions.add("{\"event_type\": \"noop\", \"event_ts\": 1729794114937}");

        // {"event_type": "login", "event_ts": 1729794146402, "email": "user@example.com"}: partially shredded
        rows.add(objectRow(object("email", Variant.ofString("user@example.com")), group -> {
            field(group, "event_type").append("typed_value", Binary.fromString("login"));
            field(group, "event_ts").append("typed_value", EVENT_TS_2);
        }));
        expected.add(Variant.ofObject(ImmutableMap.of(
                utf8Slice("event_type"), Variant.ofString("login"),
                utf8Slice("event_ts"), Variant.ofTimestampMicrosUtc(EVENT_TS_2),
                utf8Slice("email"), Variant.ofString("user@example.com"))));
        descriptions.add("{\"event_type\": \"login\", \"event_ts\": 1729794146402, \"email\": \"user@example.com\"}");

        // {"error_msg": "malformed: ..."}: all shredded fields missing
        rows.add(objectRow(object("error_msg", Variant.ofString("malformed: ...")), group -> {
            field(group, "event_type");
            field(group, "event_ts");
        }));
        expected.add(Variant.ofObject(ImmutableMap.of(utf8Slice("error_msg"), Variant.ofString("malformed: ..."))));
        descriptions.add("{\"error_msg\": \"malformed: ...\"}");

        // "malformed: not an object": a string in value, typed_value null
        rows.add(valueOnlyRow(OBJECT_SCHEMA, Variant.ofString("malformed: not an object")));
        expected.add(Variant.ofString("malformed: not an object"));
        descriptions.add("\"malformed: not an object\"");

        // {"event_ts": 1729794240241, "click": "_button"}: event_type is missing
        rows.add(objectRow(object("click", Variant.ofString("_button")), group -> {
            field(group, "event_type");
            field(group, "event_ts").append("typed_value", EVENT_TS_3);
        }));
        expected.add(Variant.ofObject(ImmutableMap.of(
                utf8Slice("event_ts"), Variant.ofTimestampMicrosUtc(EVENT_TS_3),
                utf8Slice("click"), Variant.ofString("_button"))));
        descriptions.add("{\"event_ts\": 1729794240241, \"click\": \"_button\"}");

        // {"event_type": null, "event_ts": 1729794954163}: event_type is present and null (value 00)
        rows.add(objectRow(null, group -> {
            field(group, "event_type").append("value", Binary.fromConstantByteArray(Variant.NULL_VALUE.data().getBytes()));
            field(group, "event_ts").append("typed_value", EVENT_TS_4);
        }));
        expected.add(Variant.ofObject(ImmutableMap.of(
                utf8Slice("event_type"), Variant.NULL_VALUE,
                utf8Slice("event_ts"), Variant.ofTimestampMicrosUtc(EVENT_TS_4))));
        descriptions.add("{\"event_type\": null, \"event_ts\": 1729794954163}");

        // {"event_type": "noop", "event_ts": "2024-10-24"}: event_ts is present but not a timestamp (value "2024-10-24")
        rows.add(objectRow(null, group -> {
            field(group, "event_type").append("typed_value", Binary.fromString("noop"));
            field(group, "event_ts").append("value", Binary.fromConstantByteArray(Variant.ofString("2024-10-24").data().getBytes()));
        }));
        expected.add(event(Variant.ofString("noop"), Variant.ofString("2024-10-24")));
        descriptions.add("{\"event_type\": \"noop\", \"event_ts\": \"2024-10-24\"}");

        // { }: present but empty
        rows.add(objectRow(null, group -> {
            field(group, "event_type");
            field(group, "event_ts");
        }));
        expected.add(Variant.EMPTY_OBJECT);
        descriptions.add("{ }");

        // null: value 00, typed_value null
        rows.add(valueOnlyRow(OBJECT_SCHEMA, Variant.NULL_VALUE));
        expected.add(Variant.NULL_VALUE);
        descriptions.add("null");

        // missing: value and typed_value both null. At the top level that reads as a Variant null.
        SimpleGroup missing = new SimpleGroup(OBJECT_SCHEMA);
        missing.addGroup("v").append("metadata", Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes()));
        rows.add(missing);
        expected.add(Variant.NULL_VALUE);
        descriptions.add("missing");

        // INVALID: {"event_type": "login", "event_ts": 1729795057774}: event_type is also in value; typed_value wins
        rows.add(objectRow(object("event_type", Variant.ofString("login")), group -> {
            field(group, "event_type").append("typed_value", Binary.fromString("login"));
            field(group, "event_ts").append("typed_value", EVENT_TS_5);
        }));
        expected.add(event(Variant.ofString("login"), Variant.ofTimestampMicrosUtc(EVENT_TS_5)));
        descriptions.add("INVALID: {\"event_type\": \"login\", \"event_ts\": 1729795057774}");

        // INVALID: {"event_type": "login"}: only in value, typed_value null; the reader returns what value holds
        rows.add(valueOnlyRow(OBJECT_SCHEMA, object("event_type", Variant.ofString("login"))));
        expected.add(Variant.ofObject(ImmutableMap.of(utf8Slice("event_type"), Variant.ofString("login"))));
        descriptions.add("INVALID: {\"event_type\": \"login\"}");

        // INVALID: "a": value is not an object but typed_value is non-null; typed_value wins, an empty object
        SimpleGroup invalidString = objectRow(Variant.ofString("a"), group -> {
            field(group, "event_type");
            field(group, "event_ts");
        });
        rows.add(invalidString);
        expected.add(Variant.EMPTY_OBJECT);
        descriptions.add("INVALID: \"a\"");

        // INVALID: {}: the object has no shredded fields and typed_value is null; value is read as it stands
        rows.add(valueOnlyRow(OBJECT_SCHEMA, Variant.EMPTY_OBJECT));
        expected.add(Variant.EMPTY_OBJECT);
        descriptions.add("INVALID: {}");

        assertTable(OBJECT_SCHEMA, rows, expected, descriptions);
    }

    private static void assertTable(MessageType schema, List<Group> rows, List<Variant> expected, List<String> descriptions)
    {
        Path directory = createTempDirectory();
        try {
            File file = writeGroupFile(directory, "table.parquet", schema, rows);
            // The expected values are built from the table's first column, so a single read is compared to them
            assertLogicallyEqual(readFile(file), expected, descriptions);
            // and again in small batches, so the rows are not all in one block
            assertLogicallyEqual(readFile(file, 3), expected, descriptions);
        }
        finally {
            deleteDirectory(directory);
        }
    }

    private static Variant event(Variant eventType, Variant eventTs)
    {
        return Variant.ofObject(ImmutableMap.of(utf8Slice("event_type"), eventType, utf8Slice("event_ts"), eventTs));
    }

    private static Variant object(String key, Variant value)
    {
        return Variant.ofObject(ImmutableMap.of(utf8Slice(key), value));
    }

    // A row whose variant has a value only; the dictionary in metadata is the value's own
    private static SimpleGroup valueOnlyRow(MessageType schema, Variant value)
    {
        return variantRow(schema, value.metadata().toSlice().getBytes(), value.data().getBytes(), _ -> {});
    }

    private static SimpleGroup objectRow(Variant value, Consumer<Group> typedValueFields)
    {
        byte[] metadata = value == null ? EMPTY_METADATA.toSlice().getBytes() : value.metadata().toSlice().getBytes();
        return variantRow(OBJECT_SCHEMA, metadata, value == null ? null : value.data().getBytes(), group -> typedValueFields.accept(group.addGroup("typed_value")));
    }

    private static Group field(Group typedValue, String name)
    {
        return typedValue.addGroup(name);
    }

    private static SimpleGroup variantRow(MessageType schema, byte[] metadata, byte[] value, Consumer<Group> children)
    {
        SimpleGroup row = new SimpleGroup(schema);
        Group v = row.addGroup("v");
        v.append("metadata", Binary.fromConstantByteArray(metadata));
        if (value != null) {
            v.append("value", Binary.fromConstantByteArray(value));
        }
        children.accept(v);
        return row;
    }

    // An array element: (value, typed string), at most one of them set
    private record Element(byte[] value, String typedValue) {}

    private static Element element(byte[] value, String typedValue)
    {
        return new Element(value, typedValue);
    }

    private static SimpleGroup arrayRow(byte[] value, Element... elements)
    {
        return variantRow(ARRAY_SCHEMA, EMPTY_METADATA.toSlice().getBytes(), value, v -> {
            Group typedValue = v.addGroup("typed_value");
            for (Element element : elements) {
                Group group = typedValue.addGroup("list").addGroup("element");
                if (element.value() != null) {
                    group.append("value", Binary.fromConstantByteArray(element.value()));
                }
                if (element.typedValue() != null) {
                    group.append("typed_value", Binary.fromString(element.typedValue()));
                }
            }
        });
    }
}
