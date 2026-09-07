package com.databend.jdbc.internal.query;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Compression codecs used while reading Arrow result pages.
 *
 * <p>Arrow's commons-compress LZ4 codec copies complete compressed and decompressed buffers
 * through heap byte arrays. The JDBC read path only needs sequential decompression, so the LZ4
 * frame stream can use a fixed-size scratch buffer and avoid those full-size heap
 * intermediates.</p>
 */
final class ArrowCompressionFactory implements CompressionCodec.Factory {
    static final ArrowCompressionFactory INSTANCE = new ArrowCompressionFactory();

    private ArrowCompressionFactory() {
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
        if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
            return new DirectLz4CompressionCodec();
        }
        return CommonsCompressionFactory.INSTANCE.createCodec(codecType);
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
        if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
            return new DirectLz4CompressionCodec();
        }
        return CommonsCompressionFactory.INSTANCE.createCodec(codecType, compressionLevel);
    }

    private static final class DirectLz4CompressionCodec extends AbstractCompressionCodec {
        private static final long HEADER_SIZE = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
        private static final int STREAM_BUFFER_SIZE = 64 * 1024;

        @Override
        protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
            requireIntLength(uncompressedBuffer.writerIndex(), "uncompressed");
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (LZ4FrameOutputStream compressed = new LZ4FrameOutputStream(output)) {
                uncompressedBuffer.getBytes(0, compressed, (int) uncompressedBuffer.writerIndex());
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
            long uncompressedLength = readUncompressedLength(compressedBuffer);
            requireIntLength(uncompressedLength, "uncompressed");
            requireIntLength(compressedBuffer.writerIndex() - HEADER_SIZE, "compressed");

            ArrowBuf result = allocator.buffer(uncompressedLength);
            try (InputStream source = new ArrowBufInputStream(
                    compressedBuffer,
                    HEADER_SIZE,
                    compressedBuffer.writerIndex());
                    LZ4FrameInputStream decompressed = new LZ4FrameInputStream(source)) {
                long written = 0;
                byte[] bytes = new byte[(int) Math.min(STREAM_BUFFER_SIZE, uncompressedLength)];
                while (written < uncompressedLength) {
                    int read = decompressed.read(
                            bytes,
                            0,
                            (int) Math.min(bytes.length, uncompressedLength - written));
                    if (read <= 0) {
                        throw new IOException("Truncated LZ4 frame");
                    }
                    result.setBytes(written, bytes, 0, read);
                    written += read;
                }
                if (decompressed.read() != -1) {
                    throw new IOException("LZ4 frame size does not match the Arrow buffer header");
                }
                result.writerIndex(uncompressedLength);
                return result;
            } catch (IOException | RuntimeException e) {
                result.close();
                throw new IllegalStateException("Failed to decompress LZ4 frame", e);
            }
        }

        @Override
        public CompressionUtil.CodecType getCodecType() {
            return CompressionUtil.CodecType.LZ4_FRAME;
        }

        private static void requireIntLength(long length, String kind) {
            if (length < 0 || length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "The " + kind + " buffer size exceeds the integer limit " + Integer.MAX_VALUE);
            }
        }
    }

    private static final class ArrowBufInputStream extends InputStream {
        private final ArrowBuf buffer;
        private final long end;
        private long position;

        private ArrowBufInputStream(ArrowBuf buffer, long start, long end) {
            this.buffer = buffer;
            this.position = start;
            this.end = end;
        }

        @Override
        public int read() {
            if (position >= end) {
                return -1;
            }
            return buffer.getByte(position++) & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (position >= end) {
                return -1;
            }
            int bytesToRead = (int) Math.min(length, end - position);
            buffer.getBytes(position, bytes, offset, bytesToRead);
            position += bytesToRead;
            return bytesToRead;
        }
    }
}
