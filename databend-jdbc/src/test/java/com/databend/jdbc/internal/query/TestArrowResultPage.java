package com.databend.jdbc.internal.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public void testTransferBatchMovesBuffersIntoResultPage() throws Exception {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator allocator = rootAllocator.newChildAllocator("test-arrow-transfer", 0, Long.MAX_VALUE);
        Field intField = new Field("n", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field textField = new Field("text", FieldType.nullable(new ArrowType.Utf8()), null);
        Field childField = new Field("child", FieldType.nullable(new ArrowType.Utf8()), null);
        Field structField = new Field(
                "nested",
                FieldType.nullable(new ArrowType.Struct()),
                Collections.singletonList(childField));
        VectorSchemaRoot source = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(Arrays.asList(intField, textField, structField)),
                allocator);
        source.allocateNew();
        ((IntVector) source.getVector(0)).setSafe(0, 7);
        ((IntVector) source.getVector(0)).setSafe(1, 8);
        ((IntVector) source.getVector(0)).setSafe(2, 9);
        VarCharVector textVector = (VarCharVector) source.getVector(1);
        textVector.setSafe(0, "alpha".getBytes(StandardCharsets.UTF_8));
        textVector.setSafe(2, "gamma".getBytes(StandardCharsets.UTF_8));
        StructVector structVector = (StructVector) source.getVector(2);
        VarCharVector childVector = (VarCharVector) structVector.getChild("child");
        structVector.setIndexDefined(0);
        structVector.setIndexDefined(1);
        structVector.setIndexDefined(2);
        childVector.setSafe(0, "nested-a".getBytes(StandardCharsets.UTF_8));
        childVector.setSafe(1, "nested-b".getBytes(StandardCharsets.UTF_8));
        childVector.setSafe(2, "nested-c".getBytes(StandardCharsets.UTF_8));
        source.setRowCount(3);
        long intDataAddress = source.getVector(0).getDataBuffer().memoryAddress();
        long textDataAddress = textVector.getDataBuffer().memoryAddress();
        long childDataAddress = childVector.getDataBuffer().memoryAddress();
        long allocatedBeforeTransfer = rootAllocator.getAllocatedMemory();

        VectorSchemaRoot transferred = ArrowResultPage.transferBatch(source, allocator);
        Assert.assertEquals(transferred.getVector(0).getDataBuffer().memoryAddress(), intDataAddress);
        Assert.assertEquals(transferred.getVector(1).getDataBuffer().memoryAddress(), textDataAddress);
        StructVector transferredStruct = (StructVector) transferred.getVector(2);
        VarCharVector transferredChild = (VarCharVector) transferredStruct.getChild("child");
        Assert.assertEquals(transferredChild.getDataBuffer().memoryAddress(), childDataAddress);
        Assert.assertEquals(source.getVector(0).getDataBuffer().capacity(), 0L);
        Assert.assertEquals(textVector.getDataBuffer().capacity(), 0L);
        Assert.assertEquals(childVector.getDataBuffer().capacity(), 0L);
        Assert.assertEquals(rootAllocator.getAllocatedMemory(), allocatedBeforeTransfer);
        source.close();
        ArrowResultPage page = new ArrowResultPage(
                allocator,
                Collections.singletonList(transferred),
                Collections.emptyMap());

        Assert.assertEquals(page.getRowCount(), 3);
        Assert.assertEquals(page.getValue(0, 0), 7);
        Assert.assertEquals(page.getValue(1, 0), 8);
        Assert.assertEquals(page.getValue(2, 0), 9);
        Assert.assertEquals(page.getValue(0, 1), "alpha");
        Assert.assertNull(page.getValue(1, 1));
        Assert.assertEquals(page.getValue(2, 1), "gamma");
        Map<?, ?> nested = (Map<?, ?>) page.getValue(0, 2);
        Assert.assertEquals(nested.get("child").toString(), "nested-a");
        page.close();
        Assert.assertEquals(rootAllocator.getAllocatedMemory(), 0L);
        rootAllocator.close();
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
