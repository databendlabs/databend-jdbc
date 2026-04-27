package com.databend.jdbc.internal.query;

import com.databend.jdbc.IntervalValue;
import com.databend.jdbc.internal.data.DatabendRawType;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

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
    private final Map<String, String> settings;
    private final AtomicBoolean closed = new AtomicBoolean();

    ArrowResultPage(BufferAllocator allocator, List<VectorSchemaRoot> batches, Map<String, String> settings) {
        this.allocator = allocator;
        this.batches = batches;
        this.settings = settings == null ? Collections.<String, String>emptyMap() : settings;
        this.rowOffsets = new int[batches.size()];
        int offset = 0;
        for (int i = 0; i < batches.size(); i++) {
            this.rowOffsets[i] = offset;
            offset += batches.get(i).getRowCount();
        }
    }

    @Override
    public int getRowCount() {
        if (batches.isEmpty()) {
            return 0;
        }
        return rowOffsets[batches.size() - 1] + batches.get(batches.size() - 1).getRowCount();
    }

    @Override
    public Object getValue(int rowIndex, int columnIndex) throws SQLException {
        int batchIndex = 0;
        while (batchIndex + 1 < rowOffsets.length && rowOffsets[batchIndex + 1] <= rowIndex) {
            batchIndex++;
        }
        VectorSchemaRoot root = batches.get(batchIndex);
        int rowInBatch = rowIndex - rowOffsets[batchIndex];
        FieldVector vector = root.getVector(columnIndex);
        if (vector == null || vector.isNull(rowInBatch)) {
            return null;
        }

        Field field = vector.getField();
        String extensionType = field.getMetadata() == null ? null : field.getMetadata().get(EXTENSION_KEY);
        if (extensionType != null) {
            if (EXTENSION_TYPE_VARIANT.equals(extensionType) || EXTENSION_TYPE_BITMAP.equals(extensionType)) {
                return new String(decodeBinary(vector, rowInBatch), StandardCharsets.UTF_8);
            }
            if (EXTENSION_TYPE_GEOMETRY.equals(extensionType) || EXTENSION_TYPE_GEOGRAPHY.equals(extensionType)) {
                byte[] bytes = decodeBinary(vector, rowInBatch);
                return "wkb".equalsIgnoreCase(settings.get("geometry_output_format"))
                        ? bytes
                        : new String(bytes, StandardCharsets.UTF_8);
            }
            if (EXTENSION_TYPE_INTERVAL.equals(extensionType)) {
                DecimalParts parts = readDecimal128((DecimalVector) vector, rowInBatch);
                if (parts.months != 0) {
                    throw new SQLException("Arrow interval with year/month component is not supported by JDBC Duration");
                }
                return new IntervalValue(parts.days, parts.micros);
            }
            if (EXTENSION_TYPE_TIMESTAMP_TZ.equals(extensionType)) {
                DecimalParts parts = readDecimal128((DecimalVector) vector, rowInBatch);
                return offsetDateTimeFromMicros(parts.micros, parts.offsetSeconds);
            }
            if (EXTENSION_TYPE_VECTOR.equals(extensionType)) {
                return vector.getObject(rowInBatch);
            }
        }

        ArrowType type = field.getType();
        if (type instanceof ArrowType.Bool) {
            return vector.getObject(rowInBatch);
        }
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            if (!intType.getIsSigned()) {
                if (intType.getBitWidth() == 8) {
                    short value = ((UInt1Vector) vector).getObjectNoOverflow(rowInBatch);
                    return Short.valueOf(value);
                }
                if (intType.getBitWidth() == 16) {
                    return Integer.valueOf(((UInt2Vector) vector).getObject(rowInBatch));
                }
                if (intType.getBitWidth() == 32) {
                    return Long.valueOf(((UInt4Vector) vector).getObjectNoOverflow(rowInBatch));
                }
                if (intType.getBitWidth() == 64) {
                    return ((UInt8Vector) vector).getObject(rowInBatch);
                }
            }
            return vector.getObject(rowInBatch);
        }
        if (type instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) type;
            if (floatingPoint.getPrecision() == FloatingPointPrecision.SINGLE || floatingPoint.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return vector.getObject(rowInBatch);
            }
        }
        if (type instanceof ArrowType.Decimal) {
            return ((DecimalVector) vector).getObject(rowInBatch);
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8 || type instanceof ArrowType.Utf8View) {
            Object value = vector.getObject(rowInBatch);
            return value instanceof Text ? value.toString() : String.valueOf(value);
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary || type instanceof ArrowType.FixedSizeBinary || type instanceof ArrowType.BinaryView) {
            return decodeBinary(vector, rowInBatch);
        }
        if (type instanceof ArrowType.Date) {
            return java.sql.Date.valueOf(LocalDate.ofEpochDay(((DateDayVector) vector).get(rowInBatch)));
        }
        if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestampType = (ArrowType.Timestamp) type;
            if (timestampType.getUnit() != TimeUnit.MICROSECOND) {
                throw new SQLException("Unsupported Arrow timestamp unit: " + timestampType.getUnit());
            }
            if (timestampType.getTimezone() == null || timestampType.getTimezone().isEmpty()) {
                return ((TimeStampMicroVector) vector).getObject(rowInBatch);
            }
            return offsetDateTimeFromMicros(((Number) vector.getObject(rowInBatch)).longValue(), 0);
        }
        return vector.getObject(rowInBatch);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (VectorSchemaRoot batch : batches) {
            for (FieldVector vector : batch.getFieldVectors()) {
                vector.close();
            }
            batch.close();
        }
        allocator.close();
    }

    static ArrowResultPage fromRecordBatches(BufferAllocator allocator, org.apache.arrow.vector.types.pojo.Schema schema, List<ArrowRecordBatch> recordBatches, Map<String, String> settings) {
        List<VectorSchemaRoot> roots = new ArrayList<>(recordBatches.size());
        for (ArrowRecordBatch batch : recordBatches) {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            try {
                new VectorLoader(root, CommonsCompressionFactory.INSTANCE).load(batch);
                roots.add(root);
            } finally {
                batch.close();
            }
        }
        return new ArrowResultPage(allocator, roots, settings);
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
}
