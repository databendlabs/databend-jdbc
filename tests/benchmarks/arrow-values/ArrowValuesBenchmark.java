package com.databend.jdbc.internal.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** Standalone, offline result-page traversal benchmark. No server or HTTP timing. */
public final class ArrowValuesBenchmark {
    private static volatile long sink;

    public static void main(String[] args) throws Exception {
        int warmups = Integer.getInteger("warmups", 10);
        int iterations = Integer.getInteger("iterations", 20);
        for (boolean decimals : new boolean[] {false, true}) {
            for (int batchCount : new int[] {1, 20}) {
                try (RootAllocator root = new RootAllocator()) {
                    BufferAllocator allocator = root.newChildAllocator("benchmark", 0, Long.MAX_VALUE);
                    try (ArrowResultPage page = page(allocator, batchCount, decimals)) {
                        for (int i = 0; i < warmups; i++) {
                            sink = scan(page);
                        }
                        long started = System.nanoTime();
                        long checksum = 0;
                        for (int i = 0; i < iterations; i++) {
                            checksum += scan(page);
                        }
                        long nanos = System.nanoTime() - started;
                        sink = checksum;
                        double values = (double) iterations * page.getRowCount() * 15;
                        System.out.printf(Locale.ROOT, "%s batches=%d ns/value=%.2f checksum=%d%n",
                                decimals ? "decimal-heavy" : "primitive-text", batchCount, nanos / values, checksum);
                    }
                    if (root.getAllocatedMemory() != 0) {
                        throw new AssertionError("Leaked Arrow memory");
                    }
                }
            }
        }
    }

    private static long scan(ArrowResultPage page) throws Exception {
        long checksum = 0;
        for (int row = 0; row < page.getRowCount(); row++) {
            for (int col = 0; col < 15; col++) {
                Object value = page.getValue(row, col);
                checksum += value == null ? 0 : value.hashCode();
            }
        }
        return checksum;
    }

    private static ArrowResultPage page(BufferAllocator allocator, int batchCount, boolean decimals) {
        List<Field> fields = new ArrayList<>();
        for (int col = 0; col < 15; col++) {
            ArrowType type = decimals && col < 7 ? new ArrowType.Decimal(38, 6, 128)
                    : col % 3 == 0 ? new ArrowType.Utf8() : new ArrowType.Int(32, true);
            fields.add(new Field("c" + col, FieldType.nullable(type), null));
        }
        Schema schema = new Schema(fields);
        List<VectorSchemaRoot> batches = new ArrayList<>();
        byte[] text = "USD-result-value".getBytes(StandardCharsets.UTF_8);
        BigDecimal decimal = new BigDecimal("123456.789012");
        int rowsPerBatch = 20000 / batchCount;
        for (int batch = 0; batch < batchCount; batch++) {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            root.allocateNew();
            for (int col = 0; col < 15; col++) {
                for (int row = 0; row < rowsPerBatch; row++) {
                    if ((row + col) % 17 == 0) {
                        root.getVector(col).setNull(row);
                    } else if (root.getVector(col) instanceof DecimalVector) {
                        ((DecimalVector) root.getVector(col)).setSafe(row, decimal);
                    } else if (root.getVector(col) instanceof VarCharVector) {
                        ((VarCharVector) root.getVector(col)).setSafe(row, text);
                    } else {
                        ((IntVector) root.getVector(col)).setSafe(row, batch * rowsPerBatch + row);
                    }
                }
            }
            root.setRowCount(rowsPerBatch);
            batches.add(root);
        }
        return new ArrowResultPage(allocator, batches, Collections.emptyMap());
    }
}
