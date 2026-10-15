package com.databend.jdbc.internal.query;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.jar.JarFile;

/** Artifact-level correctness checks, compiled and run using only the shaded driver jar. */
public final class ArrowLz4JarSmoke {
    private static final String RELOCATED_PREFIX = "com.databend.jdbc.net.jpountz.";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected the shaded release jar path");
        }
        Path jarPath = Paths.get(args[0]).toRealPath();
        Path loadedFrom = Paths.get(ArrowCompressionFactory.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).toRealPath();
        if (!jarPath.equals(loadedFrom)) {
            throw new AssertionError("Codec must be loaded from the release jar, not target/classes");
        }
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            if (jar.stream().anyMatch(entry -> entry.getName().startsWith("net/jpountz/")
                    || entry.getName().contains("liblz4-java"))) {
                throw new AssertionError("Unrelocated LZ4 classes or native libraries in the release jar");
            }
        }
        // Public APIs only: these factories load implementations reflectively after minimization.
        Object lz4 = Class.forName(RELOCATED_PREFIX + "lz4.LZ4Factory").getMethod("safeInstance").invoke(null);
        Object hashes = Class.forName(RELOCATED_PREFIX + "xxhash.XXHashFactory")
                .getMethod("fastestJavaInstance").invoke(null);
        if (!lz4.toString().contains("Java") || !hashes.toString().contains("Java")) {
            throw new AssertionError("Expected Java LZ4 and xxhash backends");
        }
        for (String pattern : new String[] {"repeated", "mixed", "random"}) {
            for (int size : new int[] {512, 65536, 262144, 1048576, 6291456}) {
                checkRoundTrip(payload(size, pattern));
            }
        }
        if (Boolean.TRUE.equals(Class.forName(RELOCATED_PREFIX + "util.Native")
                .getMethod("isLoaded").invoke(null))) {
            throw new AssertionError("JNI was loaded");
        }
        System.out.println("Arrow LZ4 release jar: 15 fixtures passed; Java backends; no JNI/native/unrelocated entries");
    }

    private static void checkRoundTrip(byte[] expected) throws Exception {
        FramedLZ4CompressorOutputStream.BlockSize size = expected.length <= 64 * 1024
                ? FramedLZ4CompressorOutputStream.BlockSize.K64
                : expected.length <= 256 * 1024 ? FramedLZ4CompressorOutputStream.BlockSize.K256
                : FramedLZ4CompressorOutputStream.BlockSize.M4;
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        // Exercise the hash implementations as well as multi-block decoding in the shaded jar.
        try (FramedLZ4CompressorOutputStream output = new FramedLZ4CompressorOutputStream(frame,
                new FramedLZ4CompressorOutputStream.Parameters(size, true, true, false))) {
            output.write(expected);
        }
        byte[] bytes = frame.toByteArray();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ArrowBuf input = allocator.buffer(8 + bytes.length);
            input.writeLong(MemoryUtil.LITTLE_ENDIAN ? expected.length : Long.reverseBytes(expected.length));
            input.writeBytes(bytes);
            try (ArrowBuf output = ArrowCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME)
                    .decompress(allocator, input)) {
                byte[] actual = new byte[expected.length];
                output.getBytes(0, actual);
                if (output.writerIndex() != expected.length || !Arrays.equals(expected, actual)) {
                    throw new AssertionError("Decoded payload mismatch for " + expected.length + " bytes");
                }
            } finally {
                if (input.refCnt() > 0) {
                    input.close();
                }
            }
            if (allocator.getAllocatedMemory() != 0) {
                throw new AssertionError("Arrow memory leak");
            }
        }
    }

    private static byte[] payload(int size, String pattern) {
        byte[] bytes = new byte[size];
        new Random(429).nextBytes(bytes);
        if ("mixed".equals(pattern)) {
            Arrays.fill(bytes, size / 2, size, (byte) 7);
        } else if ("repeated".equals(pattern)) {
            for (int i = 0; i < size; i++) {
                bytes[i] = (byte) (i % 31);
            }
        }
        return bytes;
    }
}
