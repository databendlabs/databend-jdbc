package com.databend.jdbc.internal.query;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestArrowResultPage {
    @Test(groups = {"UNIT_ARROW"})
    public void testArrowPageReturnsTypedValues() throws Exception {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = rootAllocator.newChildAllocator("test-arrow-page", 0, Long.MAX_VALUE);
        ArrowResultPage page;
        Field intField = new Field("n", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field dateField = new Field("d", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        Field tsField = new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null);

        IntVector intVector = new IntVector(intField, allocator);
        intVector.allocateNew();
        intVector.set(0, 7);
        intVector.setValueCount(1);

        DateDayVector dateVector = new DateDayVector(dateField, allocator);
        dateVector.allocateNew();
        dateVector.set(0, (int) LocalDate.of(2024, 4, 16).toEpochDay());
        dateVector.setValueCount(1);

        TimeStampMicroVector tsVector = new TimeStampMicroVector(tsField, allocator);
        tsVector.allocateNew();
        org.apache.arrow.vector.holders.TimeStampMicroHolder tsHolder = new org.apache.arrow.vector.holders.TimeStampMicroHolder();
        tsHolder.value = LocalDateTime.of(2024, 4, 16, 12, 34, 56, 789000000).toInstant(ZoneOffset.UTC).toEpochMilli() * 1000;
        tsVector.setSafe(0, tsHolder);
        tsVector.setValueCount(1);

        VectorSchemaRoot root = new VectorSchemaRoot(
                Arrays.asList(intField, dateField, tsField),
                Arrays.asList(intVector, dateVector, tsVector),
                1);
        page = new ArrowResultPage(allocator, Collections.singletonList(root), Collections.emptyMap());
        Assert.assertEquals(page.getValue(0, 0), 7);
        Assert.assertEquals(page.getValue(0, 1), java.sql.Date.valueOf("2024-04-16"));
        Assert.assertEquals(page.getValue(0, 2), LocalDateTime.of(2024, 4, 16, 12, 34, 56, 789000000));
        closeAllocator(page);
        closeAllocator(rootAllocator);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testArrowPageReadsAcrossBatches() throws Exception {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = rootAllocator.newChildAllocator("test-arrow-batches", 0, Long.MAX_VALUE);
        Field field = new Field("n", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        VectorSchemaRoot first = intBatch(allocator, field, 7, 8);
        VectorSchemaRoot second = intBatch(allocator, field, 9, 10);
        ArrowResultPage page = new ArrowResultPage(allocator, Arrays.asList(first, second), Collections.emptyMap());

        Assert.assertEquals(page.getRowCount(), 4);
        Assert.assertEquals(page.getValue(0, 0), 7);
        Assert.assertEquals(page.getValue(1, 0), 8);
        Assert.assertEquals(page.getValue(2, 0), 9);
        Assert.assertEquals(page.getValue(3, 0), 10);
        Assert.assertEquals(page.getValue(0, 0), 7);

        closeAllocator(page);
        closeAllocator(rootAllocator);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testArrowPageReturnsJmeterValueTypes() throws Exception {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = rootAllocator.newChildAllocator("test-arrow-jmeter-types", 0, Long.MAX_VALUE);
        List<Field> fields = Arrays.asList(
                new Field("tiny", FieldType.notNullable(new ArrowType.Int(8, true)), null),
                new Field("int", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("bigint", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("text", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("decimal", FieldType.notNullable(new ArrowType.Decimal(38, 6, 128)), null));
        VectorSchemaRoot root = VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
        root.allocateNew();
        ((TinyIntVector) root.getVector(0)).setSafe(0, (byte) 7);
        ((IntVector) root.getVector(1)).setSafe(0, 1_234);
        ((BigIntVector) root.getVector(2)).setSafe(0, 5_678L);
        ((VarCharVector) root.getVector(3)).setSafe(0, "USD".getBytes(StandardCharsets.UTF_8));
        ((DecimalVector) root.getVector(4)).setSafe(0, new BigDecimal("123456.789012"));
        root.setRowCount(1);
        ArrowResultPage page = new ArrowResultPage(allocator, Collections.singletonList(root), Collections.emptyMap());

        Assert.assertEquals(page.getValue(0, 0), Byte.valueOf((byte) 7));
        Assert.assertEquals(page.getValue(0, 1), Integer.valueOf(1_234));
        Assert.assertEquals(page.getValue(0, 2), Long.valueOf(5_678L));
        Assert.assertEquals(page.getValue(0, 3), "USD");
        Assert.assertEquals(page.getValue(0, 4), new BigDecimal("123456.789012"));

        closeAllocator(page);
        closeAllocator(rootAllocator);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testDirectLz4CompressionRoundTrip() throws Exception {
        byte[] expected = new byte[1024 * 1024];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) (i % 31);
        }
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        CompressionCodec direct = ArrowCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
        CompressionCodec commons = CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
        assertCompressionRoundTrip(allocator, expected, direct, commons);
        assertCompressionRoundTrip(allocator, expected, commons, direct);
        closeAllocator(allocator);
    }

    public void testArrowSchemaMapsToJdbcTypes() throws Exception {
        Field intField = new Field("n", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field dateField = new Field("d", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        Field tsField = new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null);

        List<QueryRowField> fields = ArrowResultPage.schemaToFields(new org.apache.arrow.vector.types.pojo.Schema(Arrays.asList(intField, dateField, tsField)));
        Assert.assertEquals(fields.get(0).getDataType().getType(), "Int32");
        Assert.assertEquals(fields.get(1).getDataType().getType(), "Nullable(Date)");
        Assert.assertEquals(fields.get(2).getDataType().getType(), "Nullable(Timestamp)");
    }

    private static VectorSchemaRoot intBatch(BufferAllocator allocator, Field field, int... values) {
        IntVector vector = new IntVector(field, allocator);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.set(i, values[i]);
        }
        vector.setValueCount(values.length);
        return new VectorSchemaRoot(Collections.singletonList(field), Collections.singletonList(vector), values.length);
    }

    private static void assertCompressionRoundTrip(BufferAllocator allocator, byte[] expected,
            CompressionCodec compressor, CompressionCodec decompressor) {
        ArrowBuf input = allocator.buffer(expected.length);
        input.writeBytes(expected);
        ArrowBuf compressed = compressor.compress(allocator, input);
        ArrowBuf decompressed = decompressor.decompress(allocator, compressed);
        byte[] actual = new byte[expected.length];
        decompressed.getBytes(0, actual);
        Assert.assertEquals(actual, expected);
        decompressed.close();
    }

    private static void closeAllocator(AutoCloseable closeable) throws Exception {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IllegalStateException ignored) {
        }
    }
}
