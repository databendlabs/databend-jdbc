package com.databend.jdbc.internal.query;

import com.sun.management.ThreadMXBean;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.jar.JarFile;

/** Single-thread exploratory decoder benchmark; each invocation runs one variant in a fresh JVM. */
public final class ArrowLz4Benchmark {
    private static final CompressionUtil.CodecType LZ4 = CompressionUtil.CodecType.LZ4_FRAME;
    private static final String PREFIX = "com.databend.jdbc.net.jpountz.";

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "verify".equals(args[0])) {
            verifyJar();
            return;
        }
        if (args.length != 5) {
            throw new IllegalArgumentException("direct|arrow|commons-stream bytes repeated|mixed|random buffersPerBatch seconds");
        }
        String variant = args[0];
        int size = Integer.parseInt(args[1]);
        String pattern = args[2];
        int batch = Integer.parseInt(args[3]);
        long nanos = (long) (Double.parseDouble(args[4]) * 1e9);
        if (size <= 0 || batch <= 0 || nanos <= 0) {
            throw new IllegalArgumentException("Numeric arguments must be positive");
        }
        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!threads.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocation counters are required");
        }
        threads.setThreadAllocatedMemoryEnabled(true);
        byte[] expected = payload(size, pattern);
        byte[] frame = frame(expected);
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowBuf input = allocator.buffer(8 + frame.length)) {
            input.writeLong(size);
            input.writeBytes(frame);
            try (ArrowBuf output = decode(codec(variant), allocator, input)) {
                assertPayload(output, expected);
            }
            run(variant, allocator, input, batch, 400_000_000L);
            System.gc();
            long gcCount = gc(false), gcMillis = gc(true);
            long before = threads.getThreadAllocatedBytes(Thread.currentThread().getId());
            long start = System.nanoTime();
            long count = run(variant, allocator, input, batch, nanos);
            double elapsed = (System.nanoTime() - start) / 1e9;
            long allocated = threads.getThreadAllocatedBytes(Thread.currentThread().getId()) - before;
            System.out.printf(Locale.ROOT, "%s,%s,%d,%d,%d,%.1f,%.0f,%d,%d%n", variant,
                    pattern, size, frame.length, batch, count * (double) size / (1024 * 1024) / elapsed,
                    (double) allocated / count, gc(false) - gcCount, gc(true) - gcMillis);
        }
        assertJavaOnly();
    }

    private static long run(String variant, BufferAllocator allocator, ArrowBuf input, int batch, long nanos) {
        long end = System.nanoTime() + nanos;
        long count = 0;
        CompressionCodec codec = codec(variant);
        while (System.nanoTime() < end) {
            if (count != 0 && count % batch == 0) {
                codec = codec(variant);
            }
            decode(codec, allocator, input).close();
            count++;
        }
        return count;
    }

    private static CompressionCodec codec(String variant) {
        switch (variant) {
            case "direct": return ArrowCompressionFactory.INSTANCE.createCodec(LZ4);
            case "arrow": return CommonsCompressionFactory.INSTANCE.createCodec(LZ4);
            case "commons-stream": return new CommonsStreamCodec();
            default: throw new IllegalArgumentException("Unknown variant " + variant);
        }
    }

    private static ArrowBuf decode(CompressionCodec codec, BufferAllocator allocator, ArrowBuf input) {
        input.getReferenceManager().retain();
        try {
            return codec.decompress(allocator, input);
        } catch (RuntimeException | Error e) {
            input.close();
            throw e;
        }
    }

    private static byte[] payload(int size, String pattern) {
        byte[] result = new byte[size];
        new Random(429).nextBytes(result);
        switch (pattern) {
            case "random": break;
            case "mixed": Arrays.fill(result, size / 2, size, (byte) 7); break;
            case "repeated":
                for (int i = 0; i < size; i++) {
                    result[i] = (byte) (i % 31);
                }
                break;
            default: throw new IllegalArgumentException("Unknown pattern " + pattern);
        }
        return result;
    }

    private static byte[] frame(byte[] payload) throws IOException {
        FramedLZ4CompressorOutputStream.BlockSize size = payload.length <= 64 * 1024
                ? FramedLZ4CompressorOutputStream.BlockSize.K64
                : payload.length <= 256 * 1024 ? FramedLZ4CompressorOutputStream.BlockSize.K256
                : FramedLZ4CompressorOutputStream.BlockSize.M4;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (FramedLZ4CompressorOutputStream output = new FramedLZ4CompressorOutputStream(bytes,
                new FramedLZ4CompressorOutputStream.Parameters(size, false, false, false))) {
            output.write(payload);
        }
        return bytes.toByteArray();
    }

    private static void assertPayload(ArrowBuf output, byte[] expected) {
        byte[] actual = new byte[expected.length];
        output.getBytes(0, actual);
        if (output.writerIndex() != expected.length || !Arrays.equals(expected, actual)) {
            throw new AssertionError("Decoded payload mismatch");
        }
    }

    private static void assertJavaOnly() throws Exception {
        if (Boolean.TRUE.equals(Class.forName(PREFIX + "util.Native").getMethod("isLoaded").invoke(null))) {
            throw new AssertionError("JNI was loaded");
        }
    }

    private static void verifyJar() throws Exception {
        try (JarFile jar = new JarFile(ArrowCompressionFactory.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI().getPath())) {
            if (jar.stream().anyMatch(entry -> entry.getName().startsWith("net/jpountz/")
                    || entry.getName().contains("liblz4-java"))) {
                throw new AssertionError("Unrelocated classes or native libraries");
            }
        }
        // Verify reflectively loaded Java implementations survive jar minimization, using public APIs.
        Object lz4 = Class.forName(PREFIX + "lz4.LZ4Factory").getMethod("safeInstance").invoke(null);
        Object hashes = Class.forName(PREFIX + "xxhash.XXHashFactory").getMethod("fastestJavaInstance").invoke(null);
        if (!lz4.toString().contains("Java") || !hashes.toString().contains("Java")) {
            throw new AssertionError("Expected Java backends");
        }
        for (String pattern : new String[] {"repeated", "mixed", "random"}) {
            for (int size : new int[] {512, 65536, 262144, 1048576, 6291456}) {
                byte[] expected = payload(size, pattern);
                byte[] frame = frame(expected);
                try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                        ArrowBuf input = allocator.buffer(8 + frame.length)) {
                    input.writeLong(size);
                    input.writeBytes(frame);
                    try (ArrowBuf output = decode(codec("direct"), allocator, input)) {
                        assertPayload(output, expected);
                    }
                }
            }
        }
        assertJavaOnly();
        System.out.println("Shaded jar: 15 fixtures passed; Java backends; no JNI/native/unrelocated entries");
    }

    private static long gc(boolean time) {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> Math.max(0, time ? bean.getCollectionTime() : bean.getCollectionCount())).sum();
    }

    private static final class CommonsStreamCodec extends AbstractCompressionCodec {
        private final byte[] window = new byte[8 * 1024];

        @Override
        protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf input) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompressionUtil.CodecType getCodecType() {
            return LZ4;
        }

        @Override
        protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf input) {
            long length = readUncompressedLength(input);
            ArrowBuf result = allocator.buffer(length);
            boolean success = false;
            try (InputStream stream = new FramedLZ4CompressorInputStream(new BufferInput(input), true)) {
                long written = 0;
                while (written < length) {
                    int count = stream.read(window, 0, (int) Math.min(window.length, length - written));
                    if (count <= 0) {
                        throw new IOException("Truncated frame");
                    }
                    result.setBytes(written, window, 0, count);
                    written += count;
                }
                if (stream.read() != -1) {
                    throw new IOException("Excess frame data");
                }
                result.writerIndex(length);
                success = true;
                return result;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                if (!success) {
                    result.close();
                }
            }
        }
    }

    private static final class BufferInput extends InputStream {
        private final ArrowBuf buffer;
        private long position = 8;

        private BufferInput(ArrowBuf buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            return position == buffer.writerIndex() ? -1 : buffer.getByte(position++) & 255;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (position == buffer.writerIndex()) {
                return -1;
            }
            int count = (int) Math.min(length, buffer.writerIndex() - position);
            buffer.getBytes(position, bytes, offset, count);
            position += count;
            return count;
        }
    }
}
