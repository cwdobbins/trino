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
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.String.format;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils() {}

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return ImmutableList.copyOf(new ColumnIOFactory().getColumnIO(requestedSchema, fileSchema, true).getLeaves());
    }

    public static MessageColumnIO getColumnIO(MessageType fileSchema, MessageType requestedSchema)
    {
        return new ColumnIOFactory().getColumnIO(requestedSchema, fileSchema, true);
    }

    public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO)
    {
        while (groupColumnIO.getChildrenCount() == 1) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
        }
        return groupColumnIO;
    }

    /* For backward-compatibility, the type of elements in LIST-annotated structures should always be determined by the following rules:
     * 1. If the repeated field is not a group, then its type is the element type and elements are required.
     * 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
     * 3. If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name with _tuple appended then the repeated type is the element type and elements are required.
     * 4. Otherwise, the repeated field's type is the element type with the repeated field's repetition.
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     * Rule 3 applies only when elementType is a RowType, because a single-field group can only be read as a row.
     */
    public static ColumnIO getArrayElementColumn(Type elementType, ColumnIO columnIO)
    {
        while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
            columnIO = ((GroupColumnIO) columnIO).getChild(0);
        }

        /* If array has a standard 3-level structure with middle level repeated group with a single field:
         *  optional group my_list (LIST) {
         *     repeated group element {
         *        required binary str (UTF8);
         *     };
         *  }
         */
        if (columnIO instanceof GroupColumnIO groupColumnIO &&
                columnIO.getType().getLogicalTypeAnnotation() == null &&
                groupColumnIO.getChildrenCount() == 1 &&
                (!(elementType instanceof RowType) ||
                        (!columnIO.getName().equals("array") &&
                                !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")))) {
            return groupColumnIO.getChild(0);
        }

        /* Backward-compatibility support for 2-level arrays where a repeated field is not a group:
         *   optional group my_list (LIST) {
         *      repeated int32 element;
         *   }
         */
        return columnIO;
    }

    public static Map<List<String>, ColumnDescriptor> getDescriptors(MessageType fileSchema, MessageType requestedSchema)
    {
        // io.trino.parquet.reader.MetadataReader.readFooter performs lower casing of all column names in fileSchema.
        // requestedSchema also contains lower cased columns because of being derived from fileSchema.
        // io.trino.parquet.ParquetTypeUtils.getParquetTypeByName takes care of case-insensitive matching if needed.
        // Therefore, we don't need to repeat case-insensitive matching here.
        return getColumns(fileSchema, requestedSchema)
                .stream()
                .collect(toImmutableMap(
                        columnIO -> Arrays.asList(columnIO.getFieldPath()),
                        PrimitiveColumnIO::getColumnDescriptor,
                        // Same column name may occur more than once when the file is written by case-sensitive tools
                        (oldValue, _) -> oldValue));
    }

    @SuppressWarnings("deprecation")
    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        return switch (encoding) {
            case PLAIN -> ParquetEncoding.PLAIN;
            case RLE -> ParquetEncoding.RLE;
            case BYTE_STREAM_SPLIT -> ParquetEncoding.BYTE_STREAM_SPLIT;
            case BIT_PACKED -> ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY -> ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED -> ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY -> ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY -> ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY -> ParquetEncoding.RLE_DICTIONARY;
        };
    }

    public static org.apache.parquet.schema.Type getParquetTypeByName(String columnName, GroupType groupType)
    {
        if (groupType.containsField(columnName)) {
            return groupType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (org.apache.parquet.schema.Type type : groupType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }

    /**
     * Parquet column names are case-sensitive unlike Hive, which converts all column names to lowercase.
     * Therefore, when we look up columns we first check for exact match, and if that fails we look for a case-insensitive match.
     */
    public static ColumnIO lookupColumnByName(GroupColumnIO groupColumnIO, String columnName)
    {
        ColumnIO columnIO = groupColumnIO.getChild(columnName);

        if (columnIO != null) {
            return columnIO;
        }

        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(columnName)) {
                return groupColumnIO.getChild(i);
            }
        }

        return null;
    }

    @Nullable
    public static ColumnIO lookupColumnById(GroupColumnIO groupColumnIO, int columnId)
    {
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            ColumnIO child = groupColumnIO.getChild(i);
            if (child.getType().getId().intValue() == columnId) {
                return child;
            }
        }
        return null;
    }

    public static Optional<DecimalType> createDecimalType(PrimitiveField field)
    {
        if (!(field.getDescriptor().getPrimitiveType().getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation decimalLogicalType)) {
            return Optional.empty();
        }
        return Optional.of(DecimalType.createDecimalType(decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
    }

    /**
     * For optional fields:
     * <ul>
     * <li>definitionLevel == maxDefinitionLevel     =&gt; Value is defined</li>
     * <li>definitionLevel == maxDefinitionLevel - 1 =&gt; Value is null</li>
     * <li>definitionLevel &lt; maxDefinitionLevel - 1  =&gt; Value does not exist, because one of its optional parent fields is null</li>
     * </ul>
     */
    public static boolean isValueNull(boolean required, int definitionLevel, int maxDefinitionLevel)
    {
        return !required && (definitionLevel == maxDefinitionLevel - 1);
    }

    public static boolean isOptionalFieldValueNull(int definitionLevel, int maxDefinitionLevel)
    {
        return definitionLevel == maxDefinitionLevel - 1;
    }

    public static long getShortDecimalValue(byte[] bytes)
    {
        return getShortDecimalValue(bytes, 0, bytes.length);
    }

    public static long getShortDecimalValue(byte[] bytes, int startOffset, int length)
    {
        long value = 0;
        switch (length) {
            case 8:
                value |= bytes[startOffset + 7] & 0xFFL;
                // fall through
            case 7:
                value |= (bytes[startOffset + 6] & 0xFFL) << 8;
                // fall through
            case 6:
                value |= (bytes[startOffset + 5] & 0xFFL) << 16;
                // fall through
            case 5:
                value |= (bytes[startOffset + 4] & 0xFFL) << 24;
                // fall through
            case 4:
                value |= (bytes[startOffset + 3] & 0xFFL) << 32;
                // fall through
            case 3:
                value |= (bytes[startOffset + 2] & 0xFFL) << 40;
                // fall through
            case 2:
                value |= (bytes[startOffset + 1] & 0xFFL) << 48;
                // fall through
            case 1:
                value |= (bytes[startOffset] & 0xFFL) << 56;
        }
        value = value >> ((8 - length) * 8);
        return value;
    }

    public static void checkBytesFitInShortDecimal(byte[] bytes, int offset, int length, ColumnDescriptor descriptor)
    {
        int endOffset = offset + length;
        // Equivalent to expectedValue = bytes[endOffset] < 0 ? -1 : 0
        byte expectedValue = (byte) (bytes[endOffset] >> 7);
        for (int i = offset; i < endOffset; i++) {
            if (bytes[i] != expectedValue) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Could not read unscaled value %s into a short decimal from column %s",
                        new BigInteger(bytes, offset, length + Long.BYTES),
                        descriptor));
            }
        }
    }

    public static byte[] paddingBigInteger(BigInteger bigInteger, int numBytes)
    {
        byte[] bytes = bigInteger.toByteArray();
        if (bytes.length == numBytes) {
            return bytes;
        }
        byte[] result = new byte[numBytes];
        if (bigInteger.signum() < 0) {
            Arrays.fill(result, 0, numBytes - bytes.length, (byte) 0xFF);
        }
        System.arraycopy(bytes, 0, result, numBytes - bytes.length, bytes.length);
        return result;
    }

    /**
     * Assumes the parent of columnIO is a MessageColumnIO, i.e. columnIO should be a top level column in the schema.
     */
    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        return constructField(type, columnIO, true);
    }

    private static Optional<Field> constructField(Type type, ColumnIO columnIO, boolean isTopLevel)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        if (isVariantType(type, columnIO)) {
            if (!(columnIO instanceof GroupColumnIO groupColumnIo)) {
                throw new IllegalStateException("Expected columnIO to be GroupColumnIO but got %s".formatted(columnIO.getClass().getSimpleName()));
            }
            if (type == VARIANT) {
                checkShreddedChildren(columnPath(groupColumnIo), groupColumnIo.getType().asGroupType(), true);
            }
            if (type == VARIANT && groupColumnIo.getChild(ShreddedVariantField.TYPED_VALUE) != null) {
                return Optional.of(constructShreddedVariantField(type, groupColumnIo));
            }
            PrimitiveField valueField = (PrimitiveField) constructField(VARBINARY, groupColumnIo.getChild("value"), false).orElseThrow();
            PrimitiveField metadataField = (PrimitiveField) constructField(VARBINARY, groupColumnIo.getChild("metadata"), false).orElseThrow();
            if (type == VARIANT) {
                // Like row fields, the leaves are read null-suppressed and expanded to the variant positions in ParquetReader.readVariant
                return Optional.of(new VariantField(type, repetitionLevel, definitionLevel, required, valueField, metadataField));
            }
            return Optional.of(new VariantField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    // ParquetReader.readVariantAsJson walks one entry per variant position. When the variant group is optional,
                    // marking the leaves as optional gives one entry per position, with an empty entry for a null variant.
                    new PrimitiveField(valueField.getType(), false, valueField.getDescriptor(), valueField.getId()),
                    new PrimitiveField(metadataField.getType(), false, metadataField.getDescriptor(), metadataField.getId())));
        }
        if (type instanceof RowType rowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (RowType.Field rowField : fields) {
                String name = rowField.getName().orElseThrow().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name), false);
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType mapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0), false);
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1), false);
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType arrayType) {
            // Per the parquet spec (https://github.com/apache/parquet-format/blob/master/LogicalTypes.md):
            // `A repeated field that is neither contained by a LIST- or MAP-annotated group nor annotated by LIST or MAP should be interpreted as a required list of required elements
            // where the element type is the type of the field.`
            //
            // A parquet encoding for a required list of strings can be expressed in two ways, however for backwards compatibility they should be handled the same, so here we need
            // to adjust repetition and definition levels when converting ColumnIOs to Fields.
            //      1. required group colors (LIST) {
            //              repeated group list {
            //                  required string element;
            //              }
            //         }
            //      2. repeated binary colors (STRING);
            if (columnIO instanceof PrimitiveColumnIO primitiveColumnIO) {
                if (columnIO.getType().getRepetition() != REPEATED || repetitionLevel == 0 || definitionLevel == 0) {
                    throw new TrinoException(NOT_SUPPORTED, format("Unsupported schema for Parquet column (%s)", primitiveColumnIO.getColumnDescriptor()));
                }
                PrimitiveField primitiveFieldElement = new PrimitiveField(arrayType.getElementType(), true, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId());
                return Optional.of(new GroupField(type, repetitionLevel - 1, definitionLevel - 1, true, ImmutableList.of(Optional.of(primitiveFieldElement))));
            }
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(arrayType.getElementType(), groupColumnIO.getChild(0)), false);
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        if (primitiveColumnIO.getType().getRepetition() == REPEATED && isTopLevel) {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, primitiveColumnIO.getColumnDescriptor()));
        }
        return Optional.of(new PrimitiveField(type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId()));
    }

    private static boolean isVariantType(Type type, ColumnIO columnIO)
    {
        if (!(columnIO instanceof GroupColumnIO groupColumnIo)) {
            return false;
        }
        if (type == VARIANT) {
            return groupColumnIo.getChild(ShreddedVariantField.METADATA) != null &&
                    (groupColumnIo.getChild(ShreddedVariantField.VALUE) != null || groupColumnIo.getChild(ShreddedVariantField.TYPED_VALUE) != null);
        }
        return type.getBaseName().equals(JSON) &&
                groupColumnIo.getChildrenCount() == 2 &&
                groupColumnIo.getChild("value") != null &&
                groupColumnIo.getChild("metadata") != null;
    }

    /**
     * Builds the field tree for a shredded VARIANT column (a group with {@code metadata}, an optional
     * {@code value}, and a {@code typed_value} child) per the Parquet Variant shredding spec. The
     * shredded columns are modeled as a synthesized {@code RowType} struct mirroring the Parquet
     * layout so the standard nested readers can materialize them; the {@code typed_value} subtree is
     * navigated by exact name (variant field names are case-sensitive).
     */
    public static ShreddedVariantField constructShreddedVariantField(Type variantType, GroupColumnIO variantColumnIo)
    {
        checkShreddedLayout(columnPath(variantColumnIo), variantColumnIo.getType().asGroupType());
        NodeField node = buildShreddedNode(variantColumnIo, true);
        if (!(node.field() instanceof GroupField struct)) {
            throw shreddedLayoutException(variantColumnIo, "the variant is not a group");
        }
        return new ShreddedVariantField(
                variantType,
                variantColumnIo.getRepetitionLevel(),
                variantColumnIo.getDefinitionLevel(),
                variantColumnIo.getType().getRepetition() != OPTIONAL,
                struct,
                columnPath(variantColumnIo));
    }

    /**
     * Checks the whole shape of a shredded VARIANT group against the schema alone, throwing the {@link ShreddedLayoutException}
     * the reader throws for a layout it cannot read. The reader runs it before building fields, and a pruned read
     * runs it on the file's full group, so both reject exactly the same files.
     */
    public static void checkShreddedLayout(String columnPath, GroupType variantGroup)
    {
        checkShreddedNode(columnPath, variantGroup, true);
        // The footer lowercases schema names except below a group annotated VARIANT, so without the annotation the case of
        // a shredded object field's name, which is the key it reads as, cannot be recovered
        if (!(variantGroup.getLogicalTypeAnnotation() instanceof VariantLogicalTypeAnnotation) &&
                variantGroup.containsField(ShreddedVariantField.TYPED_VALUE) &&
                containsObjectFields(variantGroup.getType(ShreddedVariantField.TYPED_VALUE))) {
            throw shreddedLayoutException(columnPath, "the group lacks the VARIANT logical type annotation, which is required to read shredded object fields because the case of their names cannot be recovered without it");
        }
    }

    private static void checkShreddedNode(String path, GroupType node, boolean topLevel)
    {
        if (!node.containsField(ShreddedVariantField.VALUE) && !node.containsField(ShreddedVariantField.TYPED_VALUE)) {
            throw shreddedLayoutException(path, "group has neither value nor typed_value");
        }
        checkShreddedChildren(path, node, topLevel);

        if (topLevel) {
            if (!node.containsField(ShreddedVariantField.METADATA) || !isBinary(node.getType(ShreddedVariantField.METADATA))) {
                throw shreddedLayoutException(path, "metadata is not a binary column");
            }
        }
        if (node.containsField(ShreddedVariantField.VALUE)) {
            org.apache.parquet.schema.Type value = node.getType(ShreddedVariantField.VALUE);
            if (!isBinary(value)) {
                throw shreddedLayoutException(path + "." + ShreddedVariantField.VALUE, "value is not a binary column");
            }
        }
        if (node.containsField(ShreddedVariantField.TYPED_VALUE)) {
            checkShreddedTypedValue(path + "." + ShreddedVariantField.TYPED_VALUE, node.getType(ShreddedVariantField.TYPED_VALUE));
        }
    }

    private static void checkShreddedTypedValue(String path, org.apache.parquet.schema.Type typedValue)
    {
        if (typedValue.isPrimitive()) {
            shreddedScalarType(path, typedValue.asPrimitiveType());
            return;
        }
        GroupType group = typedValue.asGroupType();
        if (group.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            // Only the standard three-level list: LIST group, one repeated group, one element group inside it
            if (group.getFieldCount() != 1 ||
                    group.getType(0).isPrimitive() ||
                    group.getType(0).getRepetition() != REPEATED ||
                    group.getType(0).asGroupType().getFieldCount() != 1 ||
                    !group.getType(0).asGroupType().getType(0).getName().equals("element") ||
                    group.getType(0).asGroupType().getType(0).isPrimitive()) {
                throw shreddedLayoutException(path, "array is not a three-level list (LIST group, repeated group, element group)");
            }
            GroupType repeated = group.getType(0).asGroupType();
            checkShreddedNode(path + "." + repeated.getName() + ".element", repeated.getType(0).asGroupType(), false);
            return;
        }
        if (group.getFieldCount() == 0) {
            throw shreddedLayoutException(path, "object has no fields");
        }
        for (org.apache.parquet.schema.Type field : group.getFields()) {
            String fieldPath = path + "." + field.getName();
            if (field.isPrimitive()) {
                throw shreddedLayoutException(fieldPath, "object field '%s' is not a group holding value and typed_value".formatted(field.getName()));
            }
            checkShreddedNode(fieldPath, field.asGroupType(), false);
        }
    }

    // True when the typed_value holds an object (a group with named fields) at any depth, arrays included
    private static boolean containsObjectFields(org.apache.parquet.schema.Type typedValue)
    {
        if (typedValue.isPrimitive()) {
            return false;
        }
        GroupType group = typedValue.asGroupType();
        if (group.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            // The list's shape was checked by checkShreddedTypedValue
            GroupType element = group.getType(0).asGroupType().getType(0).asGroupType();
            return element.containsField(ShreddedVariantField.TYPED_VALUE) &&
                    containsObjectFields(element.getType(ShreddedVariantField.TYPED_VALUE));
        }
        return true;
    }

    /**
     * The one error for a shredded VARIANT layout this reader cannot read, naming the Parquet column path.
     */
    public static ShreddedLayoutException shreddedLayoutException(String columnPath, String detail)
    {
        return new ShreddedLayoutException(columnPath, detail, null);
    }

    public static ShreddedLayoutException shreddedLayoutException(String columnPath, String detail, Throwable cause)
    {
        return new ShreddedLayoutException(columnPath, detail, cause);
    }

    private static ShreddedLayoutException shreddedLayoutException(ColumnIO columnIo, String detail)
    {
        return shreddedLayoutException(columnPath(columnIo), detail);
    }

    private static String columnPath(ColumnIO columnIo)
    {
        return String.join(".", columnIo.getFieldPath());
    }

    /**
     * A variant node holds only {@code value} and {@code typed_value}, and the top-level variant also {@code metadata};
     * anything else is not a layout this reader understands, and would otherwise be ignored silently.
     */
    private static void checkShreddedChildren(String path, GroupType group, boolean topLevel)
    {
        for (org.apache.parquet.schema.Type child : group.getFields()) {
            String name = child.getName();
            boolean known = name.equals(ShreddedVariantField.VALUE) ||
                    name.equals(ShreddedVariantField.TYPED_VALUE) ||
                    (topLevel && name.equals(ShreddedVariantField.METADATA));
            if (!known) {
                throw shreddedLayoutException(path, "unexpected child '%s' beside value and typed_value".formatted(name));
            }
        }
    }

    private static NodeField buildShreddedNode(GroupColumnIO nodeColumnIo, boolean topLevel)
    {
        ImmutableList.Builder<RowType.Field> typeFields = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> childFields = ImmutableList.builder();

        // The shape was checked by checkShreddedLayout
        if (topLevel) {
            PrimitiveColumnIO metadata = (PrimitiveColumnIO) nodeColumnIo.getChild(ShreddedVariantField.METADATA);
            typeFields.add(RowType.field(ShreddedVariantField.METADATA, VARBINARY));
            childFields.add(Optional.of(shreddedPrimitiveField(VARBINARY, metadata)));
        }

        ColumnIO valueColumnIo = nodeColumnIo.getChild(ShreddedVariantField.VALUE);
        if (valueColumnIo != null) {
            PrimitiveColumnIO value = (PrimitiveColumnIO) valueColumnIo;
            typeFields.add(RowType.field(ShreddedVariantField.VALUE, VARBINARY));
            childFields.add(Optional.of(shreddedPrimitiveField(VARBINARY, value)));
        }

        ColumnIO typedValueColumnIo = nodeColumnIo.getChild(ShreddedVariantField.TYPED_VALUE);
        if (typedValueColumnIo != null) {
            NodeField typedValue = buildShreddedTypedValue(typedValueColumnIo);
            typeFields.add(RowType.field(ShreddedVariantField.TYPED_VALUE, typedValue.type()));
            childFields.add(Optional.of(typedValue.field()));
        }

        RowType rowType = RowType.from(typeFields.build());
        GroupField group = new GroupField(
                rowType,
                nodeColumnIo.getRepetitionLevel(),
                nodeColumnIo.getDefinitionLevel(),
                nodeColumnIo.getType().getRepetition() != OPTIONAL,
                childFields.build());
        return new NodeField(rowType, group);
    }

    private static boolean isBinary(org.apache.parquet.schema.Type type)
    {
        return type.isPrimitive() && type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY;
    }

    private static NodeField buildShreddedTypedValue(ColumnIO typedValueColumnIo)
    {
        if (typedValueColumnIo instanceof PrimitiveColumnIO primitive) {
            Type type = shreddedScalarType(columnPath(primitive), primitive.getType().asPrimitiveType());
            return new NodeField(type, shreddedPrimitiveField(type, primitive));
        }
        GroupColumnIO group = (GroupColumnIO) typedValueColumnIo;
        if (group.getType().getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            GroupColumnIO repeated = (GroupColumnIO) group.getChild(0);
            GroupColumnIO elementColumnIo = (GroupColumnIO) repeated.getChild(0);
            NodeField element = buildShreddedNode(elementColumnIo, false);
            ArrayType arrayType = new ArrayType(element.type());
            GroupField field = new GroupField(
                    arrayType,
                    group.getRepetitionLevel(),
                    group.getDefinitionLevel(),
                    group.getType().getRepetition() != OPTIONAL,
                    ImmutableList.of(Optional.of(element.field())));
            return new NodeField(arrayType, field);
        }

        ImmutableList.Builder<RowType.Field> typeFields = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> childFields = ImmutableList.builder();
        for (int i = 0; i < group.getChildrenCount(); i++) {
            GroupColumnIO fieldColumnIo = (GroupColumnIO) group.getChild(i);
            NodeField node = buildShreddedNode(fieldColumnIo, false);
            typeFields.add(RowType.field(fieldColumnIo.getName(), node.type()));
            childFields.add(Optional.of(node.field()));
        }
        RowType rowType = RowType.from(typeFields.build());
        GroupField field = new GroupField(
                rowType,
                group.getRepetitionLevel(),
                group.getDefinitionLevel(),
                group.getType().getRepetition() != OPTIONAL,
                childFields.build());
        return new NodeField(rowType, field);
    }

    private static PrimitiveField shreddedPrimitiveField(Type type, PrimitiveColumnIO columnIo)
    {
        return new PrimitiveField(type, columnIo.getType().getRepetition() != OPTIONAL, columnIo.getColumnDescriptor(), columnIo.getId());
    }

    private static Type shreddedScalarType(String columnPath, PrimitiveType columnIo)
    {
        LogicalTypeAnnotation annotation = columnIo.getLogicalTypeAnnotation();
        PrimitiveTypeName physicalType = columnIo.getPrimitiveTypeName();
        return switch (physicalType) {
            case BOOLEAN -> BOOLEAN;
            case FLOAT -> REAL;
            case DOUBLE -> DOUBLE;
            case INT32 -> {
                if (annotation instanceof DateLogicalTypeAnnotation) {
                    yield DATE;
                }
                if (annotation instanceof DecimalLogicalTypeAnnotation decimal) {
                    yield shreddedDecimalType(columnPath, columnIo, decimal);
                }
                if (annotation instanceof IntLogicalTypeAnnotation integer) {
                    if (!integer.isSigned()) {
                        throw unsupportedShreddedScalar(columnPath, columnIo);
                    }
                    yield switch (integer.getBitWidth()) {
                        case 8 -> TINYINT;
                        case 16 -> SMALLINT;
                        default -> INTEGER;
                    };
                }
                if (annotation instanceof TimeLogicalTypeAnnotation) {
                    // TIME(MILLIS) is not a shredded Variant type
                    throw unsupportedShreddedScalar(columnPath, columnIo);
                }
                yield INTEGER;
            }
            case INT64 -> {
                if (annotation instanceof DecimalLogicalTypeAnnotation decimal) {
                    yield shreddedDecimalType(columnPath, columnIo, decimal);
                }
                if (annotation instanceof IntLogicalTypeAnnotation integer && !integer.isSigned()) {
                    throw unsupportedShreddedScalar(columnPath, columnIo);
                }
                if (annotation instanceof TimeLogicalTypeAnnotation time) {
                    if (time.isAdjustedToUTC() || time.getUnit() != LogicalTypeAnnotation.TimeUnit.MICROS) {
                        throw unsupportedShreddedScalar(columnPath, columnIo);
                    }
                    yield TIME_MICROS;
                }
                if (annotation instanceof TimestampLogicalTypeAnnotation timestamp) {
                    yield switch (timestamp.getUnit()) {
                        case MICROS -> timestamp.isAdjustedToUTC() ? TIMESTAMP_TZ_MICROS : TIMESTAMP_MICROS;
                        case NANOS -> timestamp.isAdjustedToUTC() ? TIMESTAMP_TZ_NANOS : TIMESTAMP_NANOS;
                        case MILLIS -> throw unsupportedShreddedScalar(columnPath, columnIo);
                    };
                }
                yield BIGINT;
            }
            case BINARY -> {
                if (annotation instanceof DecimalLogicalTypeAnnotation decimal) {
                    yield shreddedDecimalType(columnPath, columnIo, decimal);
                }
                if (annotation instanceof StringLogicalTypeAnnotation) {
                    yield VARCHAR;
                }
                if (annotation != null) {
                    // JSON, BSON, ENUM and the like are not shredded Variant types; reading them as raw binary would change the value's type
                    throw unsupportedShreddedScalar(columnPath, columnIo);
                }
                yield VARBINARY;
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                if (annotation instanceof DecimalLogicalTypeAnnotation decimal) {
                    yield shreddedDecimalType(columnPath, columnIo, decimal);
                }
                if (annotation instanceof UUIDLogicalTypeAnnotation && columnIo.getTypeLength() == 16) {
                    yield UUID;
                }
                throw unsupportedShreddedScalar(columnPath, columnIo);
            }
            default -> throw unsupportedShreddedScalar(columnPath, columnIo);
        };
    }

    private static Type shreddedDecimalType(String columnPath, PrimitiveType columnIo, DecimalLogicalTypeAnnotation decimal)
    {
        if (decimal.getPrecision() > Decimals.MAX_PRECISION || decimal.getScale() > decimal.getPrecision()) {
            throw unsupportedShreddedScalar(columnPath, columnIo);
        }
        return DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
    }

    private static ShreddedLayoutException unsupportedShreddedScalar(String columnPath, PrimitiveType columnIo)
    {
        return shreddedLayoutException(columnPath, "unsupported typed_value type " + columnIo);
    }

    private record NodeField(Type type, Field field) {}
}
