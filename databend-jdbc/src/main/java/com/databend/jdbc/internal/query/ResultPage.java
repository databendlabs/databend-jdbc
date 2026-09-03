package com.databend.jdbc.internal.query;

import com.databend.jdbc.IntervalValue;
import com.databend.jdbc.internal.data.DatabendRawType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface ResultPage extends AutoCloseable {
    int getRowCount();

    Object getValue(int rowIndex, int columnIndex) throws SQLException;

    @Override
    void close();
}

final class JsonResultPage implements ResultPage {
    private final List<List<Object>> rows;

    JsonResultPage(List<List<Object>> rows) {
        this.rows = rows == null ? Collections.emptyList() : rows;
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public Object getValue(int rowIndex, int columnIndex) {
        return rows.get(rowIndex).get(columnIndex);
    }

    @Override
    public void close() {
    }
}

final class ArrowResultPage implements ResultPage {
    private static final String EXTENSION_KEY = "Extension";
    private static final String EXTENSION_TYPE_VARIANT = "Variant";
    private static final String EXTENSION_TYPE_BITMAP = "Bitmap";
    private static final String EXTENSION_TYPE_GEOMETRY = "Geometry";
    private static final String EXTENSION_TYPE_GEOGRAPHY = "Geography";
    private static final String EXTENSION_TYPE_INTERVAL = "Interval";
    private static final String EXTENSION_TYPE_VECTOR = "Vector";
    private static final String EXTENSION_TYPE_TIMESTAMP_TZ = "TimestampTz";

    private final BufferAllocator allocator;
    private final List<VectorSchemaRoot> batches;
    private final int[] rowOffsets;
    private final ValueKind[] valueKinds;
    private final boolean[] nullableColumns;
    private final int rowCount;
    private final Map<String, String> settings;
    private final AtomicBoolean closed = new AtomicBoolean();
    private int currentBatchStart;
    private int currentBatchEnd;
    private VectorSchemaRoot currentBatch;

    ArrowResultPage(BufferAllocator allocator, List<VectorSchemaRoot> batches, Map<String, String> settings) throws SQLException {
        this.allocator = allocator;
        this.batches = batches;
        this.settings = settings == null ? Collections.<String, String>emptyMap() : settings;
        this.rowOffsets = new int[batches.size()];
        int offset = 0;
        for (int i = 0; i < batches.size(); i++) {
            this.rowOffsets[i] = offset;
            offset += batches.get(i).getRowCount();
        }
        this.rowCount = offset;
        if (batches.isEmpty()) {
            this.valueKinds = new ValueKind[0];
            this.nullableColumns = new boolean[0];
        }
        else {
            List<Field> fields = batches.get(0).getSchema().getFields();
            this.valueKinds = new ValueKind[fields.size()];
            this.nullableColumns = new boolean[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                this.valueKinds[i] = valueKind(field);
                this.nullableColumns[i] = field.isNullable();
            }
        }
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public Object getValue(int rowIndex, int columnIndex) throws SQLException {
        VectorSchemaRoot root = batchForRow(rowIndex);
        int rowInBatch = rowIndex - currentBatchStart;
        FieldVector vector = root.getVector(columnIndex);
        if (vector == null || (nullableColumns[columnIndex] && vector.isNull(rowInBatch))) {
            return null;
        }

        switch (valueKinds[columnIndex]) {
            case TEXT_EXTENSION:
                return new String(decodeBinary(vector, rowInBatch), StandardCharsets.UTF_8);
            case GEOMETRY_EXTENSION:
                byte[] bytes = decodeBinary(vector, rowInBatch);
                return "wkb".equalsIgnoreCase(settings.get("geometry_output_format"))
                        ? bytes
                        : new String(bytes, StandardCharsets.UTF_8);
            case INTERVAL_EXTENSION:
                DecimalParts parts = readDecimal128((DecimalVector) vector, rowInBatch);
                if (parts.months != 0) {
                    throw new SQLException("Arrow interval with year/month component is not supported by JDBC Duration");
                }
                return new IntervalValue(parts.days, parts.micros);
            case TIMESTAMP_TZ_EXTENSION:
                DecimalParts timestampParts = readDecimal128((DecimalVector) vector, rowInBatch);
                return offsetDateTimeFromMicros(timestampParts.micros, timestampParts.offsetSeconds);
            case VECTOR_EXTENSION:
                return vector.getObject(rowInBatch);
            case BOOLEAN:
                return ((BitVector) vector).get(rowInBatch) != 0;
            case INT8:
                return ((TinyIntVector) vector).get(rowInBatch);
            case INT16:
                return ((SmallIntVector) vector).get(rowInBatch);
            case INT32:
                return ((IntVector) vector).get(rowInBatch);
            case INT64:
                return ((BigIntVector) vector).get(rowInBatch);
            case UINT8:
                return Short.valueOf(((UInt1Vector) vector).getObjectNoOverflow(rowInBatch));
            case UINT16:
                return Integer.valueOf(((UInt2Vector) vector).getObject(rowInBatch));
            case UINT32:
                return Long.valueOf(((UInt4Vector) vector).getObjectNoOverflow(rowInBatch));
            case UINT64:
                return ((UInt8Vector) vector).getObject(rowInBatch);
            case FLOAT32:
                return ((Float4Vector) vector).get(rowInBatch);
            case FLOAT64:
                return ((Float8Vector) vector).get(rowInBatch);
            case DECIMAL:
                return ((DecimalVector) vector).getObject(rowInBatch);
            case UTF8:
                return new String(((VarCharVector) vector).get(rowInBatch), StandardCharsets.UTF_8);
            case LARGE_UTF8:
                return new String(((LargeVarCharVector) vector).get(rowInBatch), StandardCharsets.UTF_8);
            case UTF8_VIEW:
                return new String(((ViewVarCharVector) vector).get(rowInBatch), StandardCharsets.UTF_8);
            case BINARY:
                return decodeBinary(vector, rowInBatch);
            case DATE:
                return java.sql.Date.valueOf(LocalDate.ofEpochDay(((DateDayVector) vector).get(rowInBatch)));
            case TIMESTAMP:
                return ((TimeStampMicroVector) vector).getObject(rowInBatch);
            case TIMESTAMP_TZ:
                return offsetDateTimeFromMicros(((Number) vector.getObject(rowInBatch)).longValue(), 0);
            case GENERIC:
            default:
                return vector.getObject(rowInBatch);
        }
    }

    private VectorSchemaRoot batchForRow(int rowIndex) throws SQLException {
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new SQLException("Invalid row index: " + rowIndex);
        }
        if (currentBatch != null && rowIndex >= currentBatchStart && rowIndex < currentBatchEnd) {
            return currentBatch;
        }

        int low = 0;
        int high = rowOffsets.length;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (rowOffsets[middle] <= rowIndex) {
                low = middle + 1;
            }
            else {
                high = middle;
            }
        }
        int batchIndex = low - 1;
        currentBatch = batches.get(batchIndex);
        currentBatchStart = rowOffsets[batchIndex];
        currentBatchEnd = currentBatchStart + currentBatch.getRowCount();
        return currentBatch;
    }

    private static ValueKind valueKind(Field field) throws SQLException {
        String extensionType = field.getMetadata() == null ? null : field.getMetadata().get(EXTENSION_KEY);
        if (extensionType != null) {
            if (EXTENSION_TYPE_VARIANT.equals(extensionType) || EXTENSION_TYPE_BITMAP.equals(extensionType)) {
                return ValueKind.TEXT_EXTENSION;
            }
            if (EXTENSION_TYPE_GEOMETRY.equals(extensionType) || EXTENSION_TYPE_GEOGRAPHY.equals(extensionType)) {
                return ValueKind.GEOMETRY_EXTENSION;
            }
            if (EXTENSION_TYPE_INTERVAL.equals(extensionType)) {
                return ValueKind.INTERVAL_EXTENSION;
            }
            if (EXTENSION_TYPE_TIMESTAMP_TZ.equals(extensionType)) {
                return ValueKind.TIMESTAMP_TZ_EXTENSION;
            }
            if (EXTENSION_TYPE_VECTOR.equals(extensionType)) {
                return ValueKind.VECTOR_EXTENSION;
            }
            throw new SQLException("Unsupported Arrow extension field: " + field);
        }

        ArrowType type = field.getType();
        if (type instanceof ArrowType.Bool) {
            return ValueKind.BOOLEAN;
        }
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            if (intType.getIsSigned()) {
                switch (intType.getBitWidth()) {
                    case 8:
                        return ValueKind.INT8;
                    case 16:
                        return ValueKind.INT16;
                    case 32:
                        return ValueKind.INT32;
                    case 64:
                        return ValueKind.INT64;
                    default:
                        return ValueKind.GENERIC;
                }
            }
            switch (intType.getBitWidth()) {
                case 8:
                    return ValueKind.UINT8;
                case 16:
                    return ValueKind.UINT16;
                case 32:
                    return ValueKind.UINT32;
                case 64:
                    return ValueKind.UINT64;
                default:
                    return ValueKind.GENERIC;
            }
        }
        if (type instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) type;
            return floatingPoint.getPrecision() == FloatingPointPrecision.SINGLE
                    ? ValueKind.FLOAT32
                    : ValueKind.FLOAT64;
        }
        if (type instanceof ArrowType.Decimal) {
            return ValueKind.DECIMAL;
        }
        if (type instanceof ArrowType.Utf8) {
            return ValueKind.UTF8;
        }
        if (type instanceof ArrowType.LargeUtf8) {
            return ValueKind.LARGE_UTF8;
        }
        if (type instanceof ArrowType.Utf8View) {
            return ValueKind.UTF8_VIEW;
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary
                || type instanceof ArrowType.FixedSizeBinary || type instanceof ArrowType.BinaryView) {
            return ValueKind.BINARY;
        }
        if (type instanceof ArrowType.Date) {
            return ValueKind.DATE;
        }
        if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
            if (timestamp.getUnit() != TimeUnit.MICROSECOND) {
                throw new SQLException("Unsupported Arrow timestamp unit: " + timestamp.getUnit());
            }
            return timestamp.getTimezone() == null || timestamp.getTimezone().isEmpty()
                    ? ValueKind.TIMESTAMP
                    : ValueKind.TIMESTAMP_TZ;
        }
        return ValueKind.GENERIC;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (VectorSchemaRoot batch : batches) {
            batch.close();
        }
        allocator.close();
    }

    static VectorSchemaRoot transferBatch(VectorSchemaRoot source, BufferAllocator allocator) {
        VectorSchemaRoot target = VectorSchemaRoot.create(source.getSchema(), allocator);
        List<FieldVector> sourceVectors = source.getFieldVectors();
        List<FieldVector> targetVectors = target.getFieldVectors();
        for (int i = 0; i < sourceVectors.size(); i++) {
            TransferPair transferPair = sourceVectors.get(i).makeTransferPair(targetVectors.get(i));
            transferPair.transfer();
        }
        target.setRowCount(source.getRowCount());
        return target;
    }

    static List<QueryRowField> schemaToFields(org.apache.arrow.vector.types.pojo.Schema schema) throws SQLException {
        List<QueryRowField> fields = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            fields.add(new QueryRowField(field.getName(), toRawType(field)));
        }
        return fields;
    }

    private static DatabendRawType toRawType(Field field) throws SQLException {
        String extensionType = field.getMetadata() == null ? null : field.getMetadata().get(EXTENSION_KEY);
        String typeName = extensionType == null ? toRawTypeName(field) : toExtensionRawTypeName(field, extensionType);
        if (field.isNullable() && !"NULL".equalsIgnoreCase(typeName)) {
            typeName = "Nullable(" + typeName + ")";
        }
        return new DatabendRawType(typeName);
    }

    private static String toExtensionRawTypeName(Field field, String extensionType) throws SQLException {
        if (EXTENSION_TYPE_VARIANT.equals(extensionType)) {
            return "Variant";
        }
        if (EXTENSION_TYPE_BITMAP.equals(extensionType)) {
            return "Bitmap";
        }
        if (EXTENSION_TYPE_GEOMETRY.equals(extensionType)) {
            return "Geometry";
        }
        if (EXTENSION_TYPE_GEOGRAPHY.equals(extensionType)) {
            return "String";
        }
        if (EXTENSION_TYPE_INTERVAL.equals(extensionType)) {
            return "Interval";
        }
        if (EXTENSION_TYPE_TIMESTAMP_TZ.equals(extensionType)) {
            return "TIMESTAMP_TZ";
        }
        if (EXTENSION_TYPE_VECTOR.equals(extensionType)) {
            return "Array(Float32)";
        }
        throw new SQLException("Unsupported Arrow extension field: " + field);
    }

    private static String toRawTypeName(Field field) throws SQLException {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Null) {
            return "NULL";
        }
        if (type instanceof ArrowType.Bool) {
            return "Boolean";
        }
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            if (intType.getIsSigned()) {
                switch (intType.getBitWidth()) {
                    case 8:
                        return "Int8";
                    case 16:
                        return "Int16";
                    case 32:
                        return "Int32";
                    case 64:
                        return "Int64";
                    default:
                        break;
                }
            } else {
                switch (intType.getBitWidth()) {
                    case 8:
                        return "UInt8";
                    case 16:
                        return "UInt16";
                    case 32:
                        return "UInt32";
                    case 64:
                        return "UInt64";
                    default:
                        break;
                }
            }
        }
        if (type instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) type;
            return floatingPoint.getPrecision() == FloatingPointPrecision.SINGLE ? "Float32" : "Float64";
        }
        if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return "Decimal(" + decimal.getPrecision() + ", " + decimal.getScale() + ")";
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary || type instanceof ArrowType.FixedSizeBinary || type instanceof ArrowType.BinaryView) {
            return "Binary";
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8 || type instanceof ArrowType.Utf8View) {
            return "String";
        }
        if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
            return timestamp.getTimezone() == null || timestamp.getTimezone().isEmpty() ? "Timestamp" : "TIMESTAMP_TZ";
        }
        if (type instanceof ArrowType.Date) {
            return "Date";
        }
        if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList || type instanceof ArrowType.FixedSizeList) {
            if (field.getChildren().isEmpty()) {
                return "Array(String)";
            }
            return "Array(" + toRawType(field.getChildren().get(0)).getType() + ")";
        }
        if (type instanceof ArrowType.Map) {
            Field entry = field.getChildren().isEmpty() ? null : field.getChildren().get(0);
            if (entry == null || entry.getChildren().size() < 2) {
                throw new SQLException("Unsupported Arrow map field: " + field);
            }
            return "Map(" + toRawType(entry.getChildren().get(0)).getType() + ", " + toRawType(entry.getChildren().get(1)).getType() + ")";
        }
        if (type instanceof ArrowType.Struct) {
            List<String> innerTypes = new ArrayList<>(field.getChildren().size());
            for (Field child : field.getChildren()) {
                innerTypes.add(toRawType(child).getType());
            }
            return "Tuple(" + String.join(", ", innerTypes) + ")";
        }
        throw new SQLException("Unsupported Arrow field: " + field);
    }

    private static byte[] decodeBinary(FieldVector vector, int rowIndex) {
        if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(rowIndex);
        }
        if (vector instanceof LargeVarBinaryVector) {
            return ((LargeVarBinaryVector) vector).get(rowIndex);
        }
        if (vector instanceof FixedSizeBinaryVector) {
            return ((FixedSizeBinaryVector) vector).get(rowIndex);
        }
        Object value = vector.getObject(rowIndex);
        return value instanceof byte[] ? (byte[]) value : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private static OffsetDateTime offsetDateTimeFromMicros(long micros, int offsetSeconds) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos).atOffset(ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    private static DecimalParts readDecimal128(DecimalVector vector, int rowIndex) {
        ArrowBuf buf = vector.get(rowIndex);
        byte[] bytes = new byte[16];
        buf.getBytes(0, bytes);
        return new DecimalParts(littleEndianDecimal128(bytes));
    }

    private static BigInteger littleEndianDecimal128(byte[] littleEndian) {
        byte[] bigEndian = new byte[littleEndian.length];
        for (int i = 0; i < littleEndian.length; i++) {
            bigEndian[i] = littleEndian[littleEndian.length - 1 - i];
        }
        return new BigInteger(bigEndian);
    }

    private static final class DecimalParts {
        private final long micros;
        private final int offsetSeconds;
        private final int months;
        private final int days;

        private DecimalParts(BigInteger value) {
            this.micros = value.longValue();
            this.offsetSeconds = value.shiftRight(64).intValue();
            this.months = value.shiftRight(96).intValue();
            this.days = value.shiftRight(64).intValue();
        }
    }

    private enum ValueKind {
        TEXT_EXTENSION,
        GEOMETRY_EXTENSION,
        INTERVAL_EXTENSION,
        TIMESTAMP_TZ_EXTENSION,
        VECTOR_EXTENSION,
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        FLOAT32,
        FLOAT64,
        DECIMAL,
        UTF8,
        LARGE_UTF8,
        UTF8_VIEW,
        BINARY,
        DATE,
        TIMESTAMP,
        TIMESTAMP_TZ,
        GENERIC
    }
}
