package com.databend.jdbc.internal.query;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Compression codecs used while reading Arrow result pages.
 *
 * <p>Arrow's Commons LZ4 codec materializes whole buffers on the heap. This codec decodes independent
 * blocks directly between ByteBuffer views of the source and destination ArrowBufs using lz4-java's
 * Java backend, without staging compressed or decompressed blocks in heap arrays. Optional content
 * checksums use a fixed 8 KiB window. Linked compressed blocks use Commons' block decoder with up to
 * 64 KiB of frame-local history; framing, checksums and output bounds remain on the same validation
 * path for both independent and dependent blocks.</p>
 */
final class ArrowCompressionFactory implements CompressionCodec.Factory {
    static final ArrowCompressionFactory INSTANCE = new ArrowCompressionFactory();

    private ArrowCompressionFactory() {
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
        if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
            return new BoundedLz4CompressionCodec();
        }
        return CommonsCompressionFactory.INSTANCE.createCodec(codecType);
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
        if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
            return new BoundedLz4CompressionCodec();
        }
        return CommonsCompressionFactory.INSTANCE.createCodec(codecType, compressionLevel);
    }

    private static final class BoundedLz4CompressionCodec extends AbstractCompressionCodec {
        private static final long HEADER_SIZE = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
        private static final int MAGIC = 0x184D2204;
        private static final int SKIPPABLE_MAGIC_BASE = 0x184D2A50;
        private static final int SKIPPABLE_MAGIC_MASK = 0xFFFFFFF0;
        private static final int FLG_DICT_ID = 0x01;
        private static final int FLG_CONTENT_CHECKSUM = 0x04;
        private static final int FLG_CONTENT_SIZE = 0x08;
        private static final int FLG_BLOCK_CHECKSUM = 0x10;
        private static final int FLG_BLOCK_INDEPENDENCE = 0x20;
        private static final int FLG_VERSION_MASK = 0xC0;
        private static final int FLG_VERSION_1 = 0x40;
        private static final int TOKEN_UNCOMPRESSED = 0x80000000;
        private static final int COPY_WINDOW_SIZE = 8 * 1024;

        // Explicitly Java-only, including hashing: do not try loading JNI in tests or in the jar.
        private static final LZ4SafeDecompressor DECOMPRESSOR =
                LZ4Factory.safeInstance().safeDecompressor();
        private static final XXHashFactory HASH_FACTORY = XXHashFactory.fastestJavaInstance();
        private static final XXHash32 HASH = HASH_FACTORY.hash32();

        private byte[] hashWindow;

        @Override
        protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
            // Only used for codec interoperability, not JDBC reads. The base class owns the input.
            long length = uncompressedBuffer.writerIndex();
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("The uncompressed buffer size exceeds the integer limit");
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (FramedLZ4CompressorOutputStream compressed = new FramedLZ4CompressorOutputStream(output)) {
                uncompressedBuffer.getBytes(0, compressed, (int) length);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to compress LZ4 frame", e);
            }
            byte[] bytes = output.toByteArray();
            ArrowBuf result = allocator.buffer(HEADER_SIZE + bytes.length);
            result.setBytes(HEADER_SIZE, bytes);
            result.writerIndex(HEADER_SIZE + bytes.length);
            return result;
        }

        @Override
        protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
            long length = readUncompressedLength(compressedBuffer);
            if (length < 0 || length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("The uncompressed buffer size exceeds the integer limit");
            }
            ArrowBuf result = allocator.buffer(length);
            boolean success = false;
            try {
                long written = decodeFrames(compressedBuffer, result, length);
                if (written != length) {
                    throw new IOException("LZ4 frames produced " + written
                            + " bytes but the Arrow buffer header declares " + length);
                }
                result.writerIndex(length);
                success = true;
                return result;
            } catch (IOException | RuntimeException e) {
                throw new IllegalStateException("Failed to decompress LZ4 frame", e);
            } finally {
                // Also release the off-heap output if an Error interrupts decoding.
                if (!success) {
                    result.close();
                }
            }
        }

        @Override
        public CompressionUtil.CodecType getCodecType() {
            return CompressionUtil.CodecType.LZ4_FRAME;
        }

        private long decodeFrames(ArrowBuf source, ArrowBuf destination, long length) throws IOException {
            Cursor input = new Cursor(source);
            long written = 0;
            boolean foundFrame = false;
            while (input.position < input.end) {
                int magic = input.readInt();
                if ((magic & SKIPPABLE_MAGIC_MASK) == SKIPPABLE_MAGIC_BASE) {
                    input.skip(Integer.toUnsignedLong(input.readInt()));
                    continue;
                }
                if (magic != MAGIC) {
                    throw new IOException("Not an LZ4 frame");
                }
                foundFrame = true;
                long descriptorStart = input.position;
                int flg = input.readByte();
                int bd = input.readByte();
                if ((flg & FLG_VERSION_MASK) != FLG_VERSION_1 || (flg & 0x02) != 0) {
                    throw new IOException("Unsupported LZ4 frame version or reserved flags");
                }
                if ((bd & 0x8F) != 0) {
                    throw new IOException("Reserved LZ4 block descriptor bits are set");
                }
                int maxBlockSize = blockMaximumSize((bd >>> 4) & 7);
                long contentSize = -1;
                if ((flg & FLG_CONTENT_SIZE) != 0) {
                    contentSize = input.readLong();
                    if (contentSize < 0 || contentSize > length - written) {
                        throw new IOException("LZ4 content size exceeds the remaining Arrow buffer length");
                    }
                }
                if ((flg & FLG_DICT_ID) != 0) {
                    throw new IOException("LZ4 preset dictionaries are unsupported");
                }
                int descriptorLength = (int) (input.position - descriptorStart);
                int expectedHeaderHash = (hash(source, descriptorStart, descriptorLength) >>> 8) & 0xFF;
                if (input.readByte() != expectedHeaderHash) {
                    throw new IOException("LZ4 frame descriptor checksum mismatch");
                }
                written = decodeBlocks(input, destination, written, length, maxBlockSize, flg, contentSize);
            }
            if (!foundFrame) {
                throw new IOException("Missing LZ4 data frame");
            }
            return written;
        }

        private long decodeBlocks(Cursor input, ArrowBuf destination, long written, long length,
                int maxBlockSize, int flg, long contentSize) throws IOException {
            boolean blockChecksum = (flg & FLG_BLOCK_CHECKSUM) != 0;
            long frameStart = written;
            boolean independent = (flg & FLG_BLOCK_INDEPENDENCE) != 0;
            byte[] linkedWindow = null;
            try (StreamingXXHash32 contentHash = (flg & FLG_CONTENT_CHECKSUM) != 0
                    ? HASH_FACTORY.newStreamingHash32(0) : null) {
                while (true) {
                    int token = input.readInt();
                    if (token == 0) {
                        break;
                    }
                    int blockSize = token & ~TOKEN_UNCOMPRESSED;
                    if (blockSize > maxBlockSize) {
                        throw new IOException("LZ4 block exceeds the declared maximum block size");
                    }
                    input.require(blockSize + (blockChecksum ? 4L : 0L));
                    if (blockChecksum && hash(input.buffer, input.position, blockSize)
                            != readIntLittleEndian(input.buffer, input.position + blockSize)) {
                        throw new IOException("LZ4 block checksum mismatch");
                    }
                    int produced;
                    if ((token & TOKEN_UNCOMPRESSED) != 0) {
                        if (blockSize > length - written) {
                            throw new IOException("LZ4 block exceeds the Arrow buffer length");
                        }
                        destination.setBytes(written, input.buffer, input.position, blockSize);
                        produced = blockSize;
                    } else {
                        int maxOut = (int) Math.min(maxBlockSize, length - written);
                        if (independent) {
                            ByteBuffer in = input.buffer.nioBuffer(input.position, blockSize);
                            ByteBuffer out = destination.nioBuffer(written, maxOut);
                            produced = DECOMPRESSOR.decompress(in, in.position(), blockSize,
                                    out, out.position(), maxOut);
                        } else {
                            if (linkedWindow == null) {
                                linkedWindow = new byte[COPY_WINDOW_SIZE];
                            }
                            produced = decodeLinkedBlock(input, blockSize, destination, written,
                                    frameStart, maxOut, linkedWindow);
                        }
                    }
                    input.skip(blockSize + (blockChecksum ? 4L : 0L));
                    if (contentHash != null) {
                        updateContentHash(contentHash, destination, written, produced);
                    }
                    written += produced;
                }
                if (contentHash != null && input.readInt() != contentHash.getValue()) {
                    throw new IOException("LZ4 content checksum mismatch");
                }
            }
            if (contentSize >= 0 && written - frameStart != contentSize) {
                throw new IOException("LZ4 content size does not match the decoded frame length");
            }
            return written;
        }

        private static int decodeLinkedBlock(Cursor input, int blockSize, ArrowBuf destination,
                long written, long frameStart, int maxOut, byte[] window) throws IOException {
            // Bound Commons to this block, not the remainder of the frame/Arrow buffer. Never let
            // fallback consume a following header or inherit output from a preceding frame.
            Cursor block = new Cursor(input.buffer, input.position, input.position + blockSize);
            try (BlockLZ4CompressorInputStream stream = new BlockLZ4CompressorInputStream(block)) {
                int historyLength = (int) Math.min(64 * 1024, written - frameStart);
                if (historyLength > 0) {
                    byte[] history = new byte[historyLength];
                    destination.getBytes(written - historyLength, history);
                    stream.prefill(history);
                }
                int produced = 0;
                while (produced < maxOut) {
                    int count = stream.read(window, 0, Math.min(window.length, maxOut - produced));
                    if (count < 0) {
                        break;
                    }
                    if (count == 0) {
                        throw new IOException("LZ4 block decoder made no progress");
                    }
                    destination.setBytes(written + produced, window, 0, count);
                    produced += count;
                }
                if (stream.read() != -1) {
                    throw new IOException("LZ4 block exceeds the declared maximum block size or Arrow buffer length");
                }
                if (block.position != block.end) {
                    throw new IOException("Trailing data in LZ4 compressed block");
                }
                return produced;
            }
        }

        private static int hash(ArrowBuf source, long offset, int length) {
            ByteBuffer bytes = source.nioBuffer(offset, length);
            return HASH.hash(bytes, bytes.position(), length, 0);
        }

        private void updateContentHash(StreamingXXHash32 contentHash, ArrowBuf destination,
                long offset, int length) {
            if (hashWindow == null) {
                hashWindow = new byte[COPY_WINDOW_SIZE];
            }
            for (int done = 0; done < length;) {
                int chunk = Math.min(hashWindow.length, length - done);
                destination.getBytes(offset + done, hashWindow, 0, chunk);
                contentHash.update(hashWindow, 0, chunk);
                done += chunk;
            }
        }

        private static int blockMaximumSize(int index) throws IOException {
            if (index < 4 || index > 7) {
                throw new IOException("Invalid LZ4 maximum block size");
            }
            return 1 << (8 + 2 * index);
        }

        private static int readIntLittleEndian(ArrowBuf buffer, long position) {
            int value = buffer.getInt(position);
            return MemoryUtil.LITTLE_ENDIAN ? value : Integer.reverseBytes(value);
        }

        /** Bounded reads use writerIndex, never allocator capacity or bytes past the IPC buffer. */
        private static final class Cursor extends InputStream {
            private final ArrowBuf buffer;
            private final long end;
            private long position;

            private Cursor(ArrowBuf buffer) {
                this(buffer, HEADER_SIZE, buffer.writerIndex());
            }

            private Cursor(ArrowBuf buffer, long position, long end) {
                this.buffer = buffer;
                this.position = position;
                this.end = end;
            }

            private void require(long length) throws IOException {
                if (length < 0 || length > end - position) {
                    throw new IOException("Truncated LZ4 frame");
                }
            }

            @Override
            public long skip(long length) throws IOException {
                require(length);
                position += length;
                return length;
            }

            private int readByte() throws IOException {
                require(1);
                return buffer.getByte(position++) & 0xFF;
            }

            private int readInt() throws IOException {
                require(4);
                int value = readIntLittleEndian(buffer, position);
                position += 4;
                return value;
            }

            private long readLong() throws IOException {
                require(8);
                long value = buffer.getLong(position);
                position += 8;
                return MemoryUtil.LITTLE_ENDIAN ? value : Long.reverseBytes(value);
            }

            @Override
            public int read() throws IOException {
                return position == end ? -1 : readByte();
            }

            @Override
            public int read(byte[] bytes, int offset, int length) {
                if (offset < 0 || length < 0 || offset > bytes.length - length) {
                    throw new IndexOutOfBoundsException();
                }
                if (length == 0) {
                    return 0;
                }
                if (position == end) {
                    return -1;
                }
                int count = (int) Math.min(length, end - position);
                buffer.getBytes(position, bytes, offset, count);
                position += count;
                return count;
            }
        }
    }
}
