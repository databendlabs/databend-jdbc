package com.databend.jdbc.internal.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestArrowResultPage {
    @Test(groups = {"UNIT"})
    public void testArrowPageReturnsTypedValues() throws Exception {
        try {
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
            Assert.assertEquals(page.getValue(0, 2), Timestamp.valueOf(LocalDateTime.of(2024, 4, 16, 12, 34, 56, 789000000)));
            closeAllocator(page);
            closeAllocator(rootAllocator);
        } catch (Throwable t) {
            if (isArrowAllocatorBootstrapIssue(t)) {
                throw new SkipException("Arrow allocator requires --add-opens java.base/java.nio on this JDK", t);
            }
            throw t;
        }
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

    private static boolean isArrowAllocatorBootstrapIssue(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("Failed to initialize MemoryUtil")
                    || message.contains("does not \"opens java.nio\""))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
