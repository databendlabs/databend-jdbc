package com.databend.jdbc.internal.query;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.codec.digest.XXHash32;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Random;

/** Frame validation and direct-buffer decoding, including malformed input and dirty off-heap output. */
public class TestArrowLz4Codec {
    private static CompressionCodec codec() {
        return ArrowCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testDirectBuffersAcrossSizesAndCompressibility() throws Exception {
        CompressionCodec codec = codec();
        for (int size : new int[] {512, 64 * 1024, 256 * 1024, 1024 * 1024}) {
            for (byte[] payload : new byte[][] {repeated(size), random(size)}) {
                assertDecodes(codec, frame(payload, 0x60, 7, true, false, false, -1), payload);
                assertDecodes(codec, frame(payload, 0x60, 7, false, true, true, -1), payload);
            }
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testMultiBlockFramesWithEveryChecksumCombinationAndBlockSize() throws Exception {
        byte[] payload = repeated(300 * 1024);
        for (FramedLZ4CompressorOutputStream.BlockSize size
                : FramedLZ4CompressorOutputStream.BlockSize.values()) {
            for (boolean blockChecksum : new boolean[] {false, true}) {
                for (boolean contentChecksum : new boolean[] {false, true}) {
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    FramedLZ4CompressorOutputStream.Parameters parameters =
                            new FramedLZ4CompressorOutputStream.Parameters(
                                    size, contentChecksum, blockChecksum, false);
                    try (FramedLZ4CompressorOutputStream stream =
                            new FramedLZ4CompressorOutputStream(bytes, parameters)) {
                        stream.write(payload);
                    }
                    assertDecodes(codec(), bytes.toByteArray(), payload);
                }
            }
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testMatchCopiesDoNotExposePriorOffHeapContents() throws Exception {
        // CVE-2025-66566: small/zero match offsets must not copy unwritten output bytes.
        // Based on the maintained fork's copyBeyondOutput regression cases; test the actual
        // framed decoder with deterministic nonzero off-heap output, not a zeroed byte[].
        for (byte poison : new byte[] {0x55, 0x77}) {
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE) {
                @Override
                public ArrowBuf buffer(long size) {
                    ArrowBuf buffer = super.buffer(size);
                    for (long i = 0; i < size; i++) {
                        buffer.setByte(i, poison);
                    }
                    return buffer;
                }
            }) {
                for (int offset = 0; offset < 14; offset++) {
                    for (int matchLength = offset; matchLength < 14; matchLength++) {
                        byte[] block = {
                            (byte) 0xe0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            (byte) matchLength, (byte) offset, 0,
                            (byte) 0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                        };
                        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        writeInt(bytes, 0x184D2204);
                        bytes.write(new byte[] {0x60, 0x40});
                        bytes.write((hash(new byte[] {0x60, 0x40}) >>> 8) & 0xFF);
                        writeInt(bytes, block.length);
                        bytes.write(block);
                        writeInt(bytes, 0);
                        assertDecodes(allocator, codec(), bytes.toByteArray(), new byte[34 + matchLength]);
                    }
                }
            }
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testConcatenatedAndSkippableFrames() throws Exception {
        byte[] first = repeated(1024);
        byte[] second = random(4096);
        byte[] frames = concat(skipFrame(new byte[] {1, 2}),
                frame(first, 0x60, 4, true, true, true, first.length),
                skipFrame(new byte[0]), frame(second, 0x60, 4, false, false, true, second.length),
                skipFrame(new byte[] {3}));
        assertDecodes(codec(), frames, concat(first, second));
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testOptionalContentSize() throws Exception {
        byte[] payload = repeated(1024);
        assertDecodes(codec(), frame(payload, 0x60, 4, true, false, false, payload.length), payload);
        assertRejected(frame(payload, 0x60, 4, true, false, false, payload.length - 1), payload.length,
                "content size");
        assertRejected(frame(payload, 0x60, 4, true, false, false, payload.length + 1), payload.length,
                "content size");
        byte[] noContentSize = frame(payload, 0x60, 4, true, false, false, -1);
        assertRejected(noContentSize, payload.length - 1, null);
        assertRejected(noContentSize, payload.length + 1, "Arrow buffer header");
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testRejectsHeaderChecksumCorruption() throws Exception {
        byte[] payload = repeated(1024);
        byte[] frame = frame(payload, 0x60, 4, true, false, false, -1);
        frame[6] ^= 1;
        assertRejected(frame, payload.length, "descriptor checksum");
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testRejectsReservedFlagsAndInvalidBlockDescriptors() throws Exception {
        byte[] payload = repeated(1024);
        assertRejected(frame(payload, 0x62, 4, true, false, false, -1), payload.length, "reserved");
        assertRejected(frame(payload, 0xA0, 4, true, false, false, -1), payload.length, "version");
        for (int descriptor : new int[] {0x30, 0x80, 0x41, 0xC0}) {
            byte[] frame = frame(payload, 0x60, 4, true, false, false, -1);
            frame[5] = (byte) descriptor;
            fixHeaderChecksum(frame, 2);
            assertRejected(frame, payload.length, "block");
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testRejectsUnsupportedDictionary() throws Exception {
        byte[] frame = frame(repeated(1024), 0x61, 4, true, false, false, -1);
        assertRejected(frame, 1024, "dictionaries");
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testRejectsBadBlockAndContentChecksums() throws Exception {
        byte[] payload = repeated(1024);
        byte[] blockHash = frame(payload, 0x60, 4, true, true, false, -1);
        // Last four bytes are EndMark, preceded by the block checksum.
        blockHash[blockHash.length - 8] ^= 1;
        assertRejected(blockHash, payload.length, "block checksum");
        byte[] contentHash = frame(payload, 0x60, 4, true, false, true, -1);
        contentHash[contentHash.length - 1] ^= 1;
        assertRejected(contentHash, payload.length, "content checksum");
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testRejectsOversizedBlockAndTrailingGarbage() throws Exception {
        byte[] payload = repeated(1024);
        byte[] frame = frame(payload, 0x60, 4, true, false, false, -1);
        byte[] oversized = frame.clone();
        putInt(oversized, 7, 64 * 1024 + 1);
        assertRejected(oversized, payload.length, "maximum block size");
        for (byte[] suffix : new byte[][] {{0}, {0, 0, 0, 0}, {4, 34, 77, 24}}) {
            assertRejected(concat(frame, suffix), payload.length, null);
        }
        assertRejected(concat(frame, frame), payload.length, null);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testTruncationAtEveryFrameBoundaryDoesNotLeak() throws Exception {
        byte[] payload = repeated(1024);
        byte[] frame = frame(payload, 0x60, 4, true, true, true, payload.length);
        for (int length = 0; length < frame.length; length++) {
            assertRejected(Arrays.copyOf(frame, length), payload.length, null);
        }
        assertRejected(skipFrame(new byte[3]), payload.length, "Missing");
        byte[] truncatedSkip = skipFrame(new byte[3]);
        putInt(truncatedSkip, 4, -1); // unsigned 4 GiB length, not an allocation request
        assertRejected(truncatedSkip, payload.length, "Truncated");
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testDependentBlocksKeepCommonsCompatibility() throws Exception {
        byte[] payload = repeated(300 * 1024);
        for (boolean checksum : new boolean[] {false, true}) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            FramedLZ4CompressorOutputStream.Parameters parameters =
                    new FramedLZ4CompressorOutputStream.Parameters(
                            FramedLZ4CompressorOutputStream.BlockSize.K64, checksum, checksum, true);
            try (FramedLZ4CompressorOutputStream stream =
                    new FramedLZ4CompressorOutputStream(output, parameters)) {
                stream.write(payload);
            }
            byte[] linkedFrame = output.toByteArray();
            assertDecodes(codec(), linkedFrame, payload);
            byte[] independent = frame(payload, 0x60, 7, true, false, false, -1);
            assertDecodes(codec(), concat(independent, linkedFrame), concat(payload, payload));
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testLinkedFrameContentSizeMustMatchActualOutput() throws Exception {
        byte[] payload = repeated(1024);
        for (boolean compressed : new boolean[] {false, true}) {
            for (int contentSize : new int[] {0, payload.length - 1}) {
                assertRejected(frame(payload, 0x40, 4, compressed, false, false, contentSize),
                        payload.length, "content size");
            }
            byte[] claimsNextFrameByte = frame(payload, 0x40, 4, compressed, false, false, payload.length + 1);
            assertRejected(concat(claimsNextFrameByte, frame(repeated(1), 0x60, 4, true, false, false, -1)),
                    payload.length + 1, "content size");
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testFramesFollowingLinkedFramesReceiveFullValidation() throws Exception {
        byte[] payload = repeated(1024);
        byte[] linked = frame(payload, 0x40, 4, true, false, false, -1);
        byte[] independent = frame(payload, 0x60, 4, true, false, false, -1);
        byte[] reservedDescriptor = independent.clone();
        reservedDescriptor[5] |= 1;
        fixHeaderChecksum(reservedDescriptor, 2);
        for (int precedingFrames : new int[] {1, 2}) {
            byte[] prefix = precedingFrames == 1 ? linked : concat(independent, linked);
            int length = (precedingFrames + 1) * payload.length;
            assertRejected(concat(prefix, frame(payload, 0x60, 4, true, false, false, 1023)),
                    length, "content size");
            assertRejected(concat(prefix, frame(payload, 0x40, 4, true, false, false, 1023)),
                    length, "content size");
            assertRejected(concat(prefix, frame(payload, 0x62, 4, true, false, false, -1)),
                    length, "reserved");
            assertRejected(concat(prefix, reservedDescriptor), length, "Reserved");
            assertRejected(concat(prefix, frame(payload, 0x61, 4, true, false, false, -1)),
                    length, "dictionaries");
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testLinkedStoredBlockMustNotExceedDeclaredMaximum() throws Exception {
        byte[] payload = repeated(64 * 1024 + 1);
        for (boolean blockHash : new boolean[] {false, true}) {
            assertRejected(frame(payload, 0x40, 4, false, blockHash, false, -1),
                    payload.length, "maximum block size");
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testLinkedCompressedOutputMustNotExceedDeclaredMaximum() throws Exception {
        byte[] payload = repeated(64 * 1024 + 1);
        // The encoded block is small enough; its DECOMPRESSED length exceeds the 64 KiB limit.
        assertRejected(frame(payload, 0x40, 4, true, false, false, -1),
                payload.length, "maximum block size");
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testLinkedFrameTruncationAndChecksumsDoNotLeak() throws Exception {
        byte[] payload = repeated(1024);
        byte[] linked = frame(payload, 0x40, 4, true, true, true, payload.length);
        for (int length = 0; length < linked.length; length++) {
            assertRejected(Arrays.copyOf(linked, length), payload.length, null);
        }
        for (int index = 0; index < linked.length; index++) {
            byte[] corrupt = linked.clone();
            corrupt[index] ^= 1;
            assertRejected(corrupt, payload.length, null);
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testLinkedAndIndependentFramesCanAlternate() throws Exception {
        byte[] payload = concat(random(64 * 1024), repeated(64 * 1024), random(64 * 1024), repeated(64 * 1024));
        for (boolean blockChecksum : new boolean[] {false, true}) {
            for (boolean contentChecksum : new boolean[] {false, true}) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (FramedLZ4CompressorOutputStream stream = new FramedLZ4CompressorOutputStream(bytes,
                        new FramedLZ4CompressorOutputStream.Parameters(
                                FramedLZ4CompressorOutputStream.BlockSize.K64, contentChecksum, blockChecksum, true))) {
                    stream.write(payload);
                }
                byte[] linked = bytes.toByteArray();
                byte[] independent = frame(payload, 0x60, 7, true, true, true, payload.length);
                byte[] frames = concat(skipFrame(new byte[0]), linked, independent,
                        skipFrame(new byte[] {7}), linked, independent);
                assertDecodes(codec(), frames, concat(payload, payload, payload, payload));
            }
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testLinkedDictionaryIncludesStoredBlocksButNeverPreviousFrames() throws Exception {
        byte[] history = new byte[] {'a', 'b', 'c', 'd'};
        // Match four bytes at offset 4, followed by five trailing literals. This block
        // requires the stored block's history and is invalid as a frame's FIRST block.
        byte[] block = new byte[] {0, 4, 0, 0x50, 't', 'a', 'i', 'l', '!'};
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        writeInt(bytes, 0x184D2204);
        byte[] descriptor = new byte[] {0x40, 0x40};
        bytes.write(descriptor);
        bytes.write((hash(descriptor) >>> 8) & 0xFF);
        byte[] header = bytes.toByteArray();
        writeInt(bytes, 0x80000000 | history.length);
        bytes.write(history);
        ByteArrayOutputStream compressedBlock = new ByteArrayOutputStream();
        writeInt(compressedBlock, block.length);
        compressedBlock.write(block);
        writeInt(compressedBlock, 0);
        byte[] linked = concat(bytes.toByteArray(), compressedBlock.toByteArray());
        byte[] expected = new byte[] {'a', 'b', 'c', 'd', 'a', 'b', 'c', 'd', 't', 'a', 'i', 'l', '!'};
        assertDecodes(codec(), linked, expected);
        byte[] missingHistory = concat(header, compressedBlock.toByteArray());
        assertRejected(missingHistory, 9, null);
        assertRejected(concat(linked, missingHistory), expected.length + 9, null);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testEmptyFramesAndZeroLengthStoredBlocks() throws Exception {
        byte[] payload = repeated(1024);
        for (int flags : new int[] {0x40, 0x60}) {
            byte[] emptyCompressed = frame(new byte[0], flags, 4, true, true, true, 0);
            byte[] emptyStored = frame(new byte[0], flags, 4, false, true, true, 0);
            byte[] data = frame(payload, flags, 4, true, true, true, -1);
            assertDecodes(codec(), concat(emptyCompressed, data, emptyStored, emptyCompressed), payload);
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testBitCorruptionAcrossChecksummedFrameIsRejected() throws Exception {
        byte[] payload = repeated(1024);
        byte[] frame = frame(payload, 0x60, 4, true, true, true, payload.length);
        for (int index = 0; index < frame.length; index++) {
            byte[] corrupt = frame.clone();
            corrupt[index] ^= 1;
            assertRejected(corrupt, payload.length, null);
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testMalformedInputDoesNotPoisonReusedCodec() throws Exception {
        CompressionCodec codec = codec();
        byte[] payload = repeated(1024);
        byte[] valid = frame(payload, 0x60, 4, true, true, true, -1);
        byte[] corrupt = valid.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertRejected(codec, corrupt, payload.length, "content checksum");
        assertDecodes(codec, valid, payload);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testRawAndEmptyArrowBuffersRetainBaseCodecSemantics() {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CompressionCodec codec = codec();
            byte[] expected = random(1024);
            ArrowBuf raw = allocator.buffer(8 + expected.length);
            raw.writeLong(-1);
            raw.writeBytes(expected);
            try (ArrowBuf result = codec.decompress(allocator, raw)) {
                byte[] actual = new byte[expected.length];
                result.getBytes(0, actual);
                Assert.assertEquals(actual, expected);
            }
            ArrowBuf empty = allocator.buffer(8);
            empty.writeLong(0);
            try (ArrowBuf result = codec.decompress(allocator, empty)) {
                Assert.assertEquals(result.writerIndex(), 0L);
            }
            Assert.assertEquals(allocator.getAllocatedMemory(), 0L);
        }
    }

    private static void assertDecodes(CompressionCodec codec, byte[] frame, byte[] expected) {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            assertDecodes(allocator, codec, frame, expected);
        }
    }

    private static void assertDecodes(RootAllocator allocator, CompressionCodec codec,
            byte[] frame, byte[] expected) {
        ArrowBuf input = allocator.buffer(8 + frame.length);
        input.writeLong(MemoryUtil.LITTLE_ENDIAN ? expected.length : Long.reverseBytes(expected.length));
        input.writeBytes(frame);
        try (ArrowBuf result = codec.decompress(allocator, input)) {
            Assert.assertEquals(result.writerIndex(), (long) expected.length);
            byte[] actual = new byte[expected.length];
            result.getBytes(0, actual);
            Assert.assertEquals(actual, expected);
        } finally {
            if (input.refCnt() > 0) {
                input.close();
            }
        }
        Assert.assertEquals(allocator.getAllocatedMemory(), 0L);
    }

    private static void assertRejected(byte[] frame, int length, String reason) {
        assertRejected(codec(), frame, length, reason);
    }

    private static void assertRejected(CompressionCodec codec, byte[] frame, int length, String reason) {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ArrowBuf input = allocator.buffer(8 + frame.length);
            input.writeLong(MemoryUtil.LITTLE_ENDIAN ? length : Long.reverseBytes(length));
            input.writeBytes(frame);
            long baseline = allocator.getAllocatedMemory();
            try {
                IllegalStateException error = Assert.expectThrows(IllegalStateException.class,
                        () -> {
                            try (ArrowBuf unexpected = codec.decompress(allocator, input)) {
                                Assert.fail("Expected malformed frame to be rejected");
                            }
                        });
                if (reason != null) {
                    Assert.assertTrue(error.getCause().getMessage().contains(reason), error.toString());
                }
                Assert.assertEquals(allocator.getAllocatedMemory(), baseline, "Output must be released on failure");
            } finally {
                if (input.refCnt() > 0) {
                    input.close();
                }
            }
            Assert.assertEquals(allocator.getAllocatedMemory(), 0L);
        }
    }

    // Small explicit fixtures let tests corrupt individual format fields without relying on the
    // producer and consumer making the same header/size/checksum mistake.
    private static byte[] frame(byte[] payload, int flg, int blockIndex, boolean compress,
            boolean blockHash, boolean contentHash, long contentSize) throws Exception {
        flg |= (blockHash ? 0x10 : 0) | (contentHash ? 4 : 0) | (contentSize >= 0 ? 8 : 0);
        ByteArrayOutputStream descriptor = new ByteArrayOutputStream();
        descriptor.write(flg);
        descriptor.write(blockIndex << 4);
        if (contentSize >= 0) {
            writeInt(descriptor, (int) contentSize);
            writeInt(descriptor, (int) (contentSize >>> 32));
        }
        if ((flg & 1) != 0) {
            writeInt(descriptor, 42);
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeInt(output, 0x184D2204);
        byte[] header = descriptor.toByteArray();
        output.write(header);
        output.write((hash(header) >>> 8) & 0xFF);
        byte[] block = payload;
        if (compress && payload.length == 0) {
            // An empty LZ4 block is one zero-literal token. Commons' block writer cannot
            // finish an empty input; keep this boundary fixture explicit and independent.
            block = new byte[] {0};
        } else if (compress) {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (BlockLZ4CompressorOutputStream stream = new BlockLZ4CompressorOutputStream(compressed)) {
                stream.write(payload);
            }
            block = compressed.toByteArray();
        }
        writeInt(output, block.length | (compress ? 0 : 0x80000000));
        output.write(block);
        if (blockHash) {
            writeInt(output, hash(block));
        }
        writeInt(output, 0);
        if (contentHash) {
            writeInt(output, hash(payload));
        }
        return output.toByteArray();
    }

    private static byte[] skipFrame(byte[] content) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeInt(output, 0x184D2A5F);
        writeInt(output, content.length);
        output.write(content);
        return output.toByteArray();
    }

    private static void fixHeaderChecksum(byte[] frame, int length) {
        frame[4 + length] = (byte) ((hash(Arrays.copyOfRange(frame, 4, 4 + length)) >>> 8) & 0xFF);
    }

    private static int hash(byte[] bytes) {
        XXHash32 hash = new XXHash32();
        hash.update(bytes, 0, bytes.length);
        return (int) hash.getValue();
    }

    private static void writeInt(ByteArrayOutputStream output, int value) {
        for (int i = 0; i < 4; i++) {
            output.write(value >>> (8 * i));
        }
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * i));
        }
    }

    private static byte[] repeated(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (i % 31);
        }
        return bytes;
    }

    private static byte[] random(int size) {
        byte[] bytes = new byte[size];
        new Random(429).nextBytes(bytes);
        return bytes;
    }

    private static byte[] concat(byte[]... arrays) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] bytes : arrays) {
            output.write(bytes);
        }
        return output.toByteArray();
    }
}
