package com.databend.jdbc.internal.query;

import com.databend.jdbc.IntervalValue;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class TestArrowValueDecoding {
    @DataProvider
    public Object[][] values() {
        byte[] text = "湖泊🙂".getBytes(StandardCharsets.UTF_8);
        return new Object[][] {
                value(new ArrowType.Bool(), true, v -> ((BitVector) v).setSafe(0, 1)),
                value(new ArrowType.Int(8, true), Byte.MIN_VALUE, v -> ((TinyIntVector) v).setSafe(0, Byte.MIN_VALUE)),
                value(new ArrowType.Int(16, true), Short.MIN_VALUE, v -> ((SmallIntVector) v).setSafe(0, Short.MIN_VALUE)),
                value(new ArrowType.Int(32, true), Integer.MIN_VALUE, v -> ((IntVector) v).setSafe(0, Integer.MIN_VALUE)),
                value(new ArrowType.Int(64, true), Long.MIN_VALUE, v -> ((BigIntVector) v).setSafe(0, Long.MIN_VALUE)),
                value(new ArrowType.Int(8, false), (short) 255, v -> ((UInt1Vector) v).setSafe(0, 255)),
                value(new ArrowType.Int(16, false), 65535, v -> ((UInt2Vector) v).setSafe(0, 65535)),
                value(new ArrowType.Int(32, false), 4294967295L, v -> ((UInt4Vector) v).setSafe(0, -1)),
                // Preserve the existing UInt64 representation (Long, including its signed bit pattern).
                value(new ArrowType.Int(64, false), -1L, v -> ((UInt8Vector) v).setSafe(0, -1L)),
                value(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), 1.25f,
                        v -> ((Float4Vector) v).setSafe(0, 1.25f)),
                value(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), -2.5d,
                        v -> ((Float8Vector) v).setSafe(0, -2.5d)),
                value(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), (short) 0x3c00,
                        v -> ((Float2Vector) v).setSafe(0, (short) 0x3c00)),
                value(new ArrowType.Decimal(38, 6, 128), new BigDecimal("-123456.789012"),
                        v -> ((DecimalVector) v).setSafe(0, new BigDecimal("-123456.789012"))),
                value(new ArrowType.Utf8(), "湖泊🙂", v -> ((VarCharVector) v).setSafe(0, text)),
                value(new ArrowType.Utf8(), "", v -> ((VarCharVector) v).setSafe(0, new byte[0])),
                value(new ArrowType.Utf8(), "\uFFFD(", v -> ((VarCharVector) v).setSafe(0, new byte[] {(byte) 0xc3, 0x28})),
                value(new ArrowType.LargeUtf8(), "湖泊🙂", v -> ((LargeVarCharVector) v).setSafe(0, text)),
                value(new ArrowType.Utf8View(), "湖泊🙂", v -> ((ViewVarCharVector) v).setSafe(0, text)),
                value(new ArrowType.Utf8View(), "long string outside the inline view",
                        v -> ((ViewVarCharVector) v).setSafe(0, "long string outside the inline view".getBytes(StandardCharsets.UTF_8))),
                value(new ArrowType.Null(), null, v -> { }),
                value(new ArrowType.Binary(), text, v -> ((VarBinaryVector) v).setSafe(0, text)),
                value(new ArrowType.BinaryView(), text, v -> ((ViewVarBinaryVector) v).setSafe(0, text)),
                value(new ArrowType.LargeBinary(), text, v -> ((LargeVarBinaryVector) v).setSafe(0, text)),
                value(new ArrowType.FixedSizeBinary(text.length), text, v -> ((FixedSizeBinaryVector) v).setSafe(0, text)),
                value(new ArrowType.Date(DateUnit.DAY), java.sql.Date.valueOf("1969-12-31"),
                        v -> ((DateDayVector) v).setSafe(0, -1)),
                value(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), LocalDateTime.parse("1969-12-31T23:59:59.999999"),
                        v -> ((TimeStampVector) v).setSafe(0, -1L)),
                value(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), OffsetDateTime.parse("1969-12-31T23:59:59.999999Z"),
                        v -> ((TimeStampVector) v).setSafe(0, -1L))
        };
    }

    private static Object[] value(ArrowType type, Object expected, Consumer<FieldVector> write) {
        return new Object[] {type, expected, write};
    }

    @Test(groups = "UNIT_ARROW", dataProvider = "values")
    public void testValueTypesAndNulls(ArrowType type, Object expected, Consumer<FieldVector> write) throws Exception {
        // Even a non-nullable schema must not bypass the vector's validity bitmap.
        for (boolean nullable : new boolean[] {true, false}) {
            try (RootAllocator root = new RootAllocator()) {
                BufferAllocator allocator = root.newChildAllocator("values", 0, Long.MAX_VALUE);
                Field field = new Field("v", new FieldType(nullable, type, null), null);
                VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
                batch.allocateNew();
                write.accept(batch.getVector(0));
                batch.getVector(0).setNull(1);
                batch.setRowCount(2);
                try (ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(batch), Collections.emptyMap())) {
                    Object actual = page.getValue(0, 0);
                    if (expected != null) {
                        Assert.assertEquals(actual.getClass(), expected.getClass(), type.toString());
                    }
                    Assert.assertEquals(actual, expected, type.toString());
                    Assert.assertNull(page.getValue(1, 0));
                    Assert.assertEquals(page.getValue(0, 0), expected);
                }
                Assert.assertEquals(root.getAllocatedMemory(), 0L);
            }
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testBatchLookupWithEmptyBatchesAndBackwardAccess() throws Exception {
        try (RootAllocator root = new RootAllocator()) {
            BufferAllocator allocator = root.newChildAllocator("batches", 0, Long.MAX_VALUE);
            List<VectorSchemaRoot> batches = Arrays.asList(
                    intBatch(allocator), intBatch(allocator, 7, 8), intBatch(allocator),
                    intBatch(allocator), intBatch(allocator, 9), intBatch(allocator, 10, 11), intBatch(allocator));
            try (ArrowResultPage page = new ArrowResultPage(allocator, batches, Collections.emptyMap())) {
                Assert.assertEquals(page.getRowCount(), 5);
                for (int row : new int[] {0, 1, 2, 3, 4, 0, 4, 2, 1, 3}) {
                    Assert.assertEquals(page.getValue(row, 0), 7 + row);
                }
                Assert.expectThrows(SQLException.class, () -> page.getValue(-1, 0));
                Assert.expectThrows(SQLException.class, () -> page.getValue(5, 0));
            }
            Assert.assertEquals(root.getAllocatedMemory(), 0L);
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testEmptyPages() throws Exception {
        try (RootAllocator root = new RootAllocator()) {
            for (boolean withBatch : new boolean[] {false, true}) {
                BufferAllocator allocator = root.newChildAllocator("empty", 0, Long.MAX_VALUE);
                List<VectorSchemaRoot> batches = withBatch
                        ? Arrays.asList(intBatch(allocator), intBatch(allocator)) : Collections.emptyList();
                try (ArrowResultPage page = new ArrowResultPage(allocator, batches, Collections.emptyMap())) {
                    Assert.assertEquals(page.getRowCount(), 0);
                    Assert.expectThrows(SQLException.class, () -> page.getValue(0, 0));
                }
                Assert.assertEquals(root.getAllocatedMemory(), 0L);
            }
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testBinaryExtensionsAndOutputSettings() throws Exception {
        for (String extension : Arrays.asList("Variant", "Bitmap", "Geometry", "Geography")) {
            for (String format : Arrays.asList("wkt", "WKB")) {
                try (RootAllocator root = new RootAllocator()) {
                    BufferAllocator allocator = root.newChildAllocator("extension", 0, Long.MAX_VALUE);
                    Field field = extensionField(new ArrowType.Binary(), extension);
                    VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
                    batch.allocateNew();
                    byte[] bytes = "extension-value".getBytes(StandardCharsets.UTF_8);
                    ((VarBinaryVector) batch.getVector(0)).setSafe(0, bytes);
                    batch.getVector(0).setNull(1);
                    batch.setRowCount(2);
                    try (ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(batch),
                            Collections.singletonMap("geometry_output_format", format))) {
                        boolean binary = (extension.equals("Geometry") || extension.equals("Geography")) && format.equals("WKB");
                        Assert.assertEquals(page.getValue(0, 0), binary ? bytes : "extension-value");
                        Assert.assertNull(page.getValue(1, 0));
                    }
                    Assert.assertEquals(root.getAllocatedMemory(), 0L);
                }
            }
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testDecimalExtensions() throws Exception {
        for (String extension : Arrays.asList("Interval", "TimestampTz")) {
            try (RootAllocator root = new RootAllocator()) {
                BufferAllocator allocator = root.newChildAllocator("decimal-extension", 0, Long.MAX_VALUE);
                Field field = extensionField(new ArrowType.Decimal(38, 0, 128), extension);
                VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
                batch.allocateNew();
                DecimalVector vector = (DecimalVector) batch.getVector(0);
                // High 64 bits contain days (Interval) or offset seconds (TimestampTz).
                int high = extension.equals("Interval") ? 2 : 3600;
                BigInteger packed = BigInteger.valueOf(high).shiftLeft(64).add(BigInteger.valueOf(1234567));
                vector.setSafe(0, new BigDecimal(packed));
                vector.setSafe(1, new BigDecimal(BigInteger.ONE.shiftLeft(96)));
                vector.setNull(2);
                batch.setRowCount(3);
                try (ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(batch), Collections.emptyMap())) {
                    if (extension.equals("Interval")) {
                        Assert.assertEquals(((IntervalValue) page.getValue(0, 0)).asDuration(),
                                Duration.ofDays(2).plusNanos(1234567000));
                        Assert.expectThrows(SQLException.class, () -> page.getValue(1, 0));
                    } else {
                        Assert.assertEquals(page.getValue(0, 0), OffsetDateTime.parse("1970-01-01T01:00:01.234567+01:00"));
                    }
                    Assert.assertNull(page.getValue(2, 0));
                }
                Assert.assertEquals(root.getAllocatedMemory(), 0L);
            }
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testUnknownExtensionKeepsUnderlyingTypeFallback() throws Exception {
        try (RootAllocator root = new RootAllocator()) {
            BufferAllocator allocator = root.newChildAllocator("fallback", 0, Long.MAX_VALUE);
            Field field = extensionField(new ArrowType.Int(32, true), "Unknown");
            VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
            batch.allocateNew();
            ((IntVector) batch.getVector(0)).setSafe(0, 42);
            batch.setRowCount(1);
            try (ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(batch), Collections.emptyMap())) {
                Assert.assertEquals(page.getValue(0, 0), 42);
            }
            Assert.assertEquals(root.getAllocatedMemory(), 0L);
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testListAndVectorExtensionsKeepGenericFallback() throws Exception {
        for (String extension : Arrays.asList(null, "Vector")) {
            try (RootAllocator root = new RootAllocator()) {
                BufferAllocator allocator = root.newChildAllocator("list", 0, Long.MAX_VALUE);
                Field child = new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null);
                FieldType type = new FieldType(true, new ArrowType.List(), null,
                        extension == null ? null : Collections.singletonMap("Extension", extension));
                Field field = new Field("v", type, Collections.singletonList(child));
                VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
                batch.allocateNew();
                ListVector vector = (ListVector) batch.getVector(0);
                UnionListWriter writer = vector.getWriter();
                writer.setPosition(0);
                writer.startList();
                writer.integer().writeInt(7);
                writer.integer().writeInt(8);
                writer.endList();
                writer.setPosition(1);
                writer.startList();
                writer.endList();
                vector.setNull(2);
                batch.setRowCount(3);
                try (ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(batch), Collections.emptyMap())) {
                    Assert.assertEquals(page.getValue(0, 0), Arrays.asList(7, 8));
                    Assert.assertEquals(page.getValue(1, 0), Collections.emptyList());
                    Assert.assertNull(page.getValue(2, 0));
                }
                Assert.assertEquals(root.getAllocatedMemory(), 0L);
            }
        }
    }

    @Test(groups = "UNIT_ARROW")
    public void testUnsupportedTimestampStillFailsOnlyOnNonNullRead() throws Exception {
        for (TimeUnit unit : Arrays.asList(TimeUnit.SECOND, TimeUnit.MILLISECOND, TimeUnit.NANOSECOND)) {
            try (RootAllocator root = new RootAllocator()) {
                BufferAllocator allocator = root.newChildAllocator("timestamp", 0, Long.MAX_VALUE);
                Field field = new Field("ts", FieldType.nullable(new ArrowType.Timestamp(unit, null)), null);
                VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
                batch.allocateNew();
                ((TimeStampVector) batch.getVector(0)).setSafe(0, 1L);
                batch.getVector(0).setNull(1);
                batch.setRowCount(2);
                try (ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(batch), Collections.emptyMap())) {
                    Assert.assertNull(page.getValue(1, 0));
                    SQLException failure = Assert.expectThrows(SQLException.class, () -> page.getValue(0, 0));
                    Assert.assertEquals(failure.getMessage(), "Unsupported Arrow timestamp unit: " + unit);
                }
                Assert.assertEquals(root.getAllocatedMemory(), 0L);
            }
        }
    }

    private static Field extensionField(ArrowType type, String extension) {
        return new Field("v", new FieldType(true, type, null, Collections.singletonMap("Extension", extension)), null);
    }

    private static VectorSchemaRoot intBatch(BufferAllocator allocator, int... values) {
        Field field = new Field("n", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
        root.allocateNew();
        for (int row = 0; row < values.length; row++) {
            ((IntVector) root.getVector(0)).setSafe(row, values[row]);
        }
        root.setRowCount(values.length);
        return root;
    }
}
