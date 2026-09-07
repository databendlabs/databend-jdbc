package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.http.HttpRetryPolicy;
import com.databend.jdbc.internal.http.TruncatedResponseException;
import okhttp3.Response;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.databend.jdbc.internal.query.RestQueryResultPages.QUERY_RESULTS_CODEC;

/**
 * Loaded only for Arrow responses. Keep Arrow types out of RestQueryResultPages:
 * JVM verification can resolve them even when a caller only uses JSON on Java 8.
 */
final class ArrowResponseDecoder {
    private ArrowResponseDecoder() {
    }

    static RestQueryResultPages.ResponsePayload decode(Response response, InputStream body) throws IOException, SQLException {
        BufferAllocator allocator = RootAllocatorHolder.INSTANCE.newChildAllocator("databend-jdbc-arrow-page", 0, Long.MAX_VALUE);
        List<VectorSchemaRoot> batches = new ArrayList<>();
        org.apache.arrow.vector.types.pojo.Schema schema;
        QueryResults results;
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new EofRejectingChannel(Channels.newChannel(body)),
                allocator,
                CommonsCompressionFactory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            schema = root.getSchema();
            String responseHeader = schema.getCustomMetadata().get("response_header");
            if (responseHeader == null) {
                throw new DatabendQueryException("Missing response_header metadata in Arrow payload");
            }

            results = QUERY_RESULTS_CODEC.fromJson(responseHeader);
            while (reader.loadNextBatch()) {
                batches.add(ArrowResultPage.transferBatch(root, allocator));
            }
        } catch (IOException e) {
            closeBatches(batches, e);
            closeAllocator(allocator, e);
            if (HttpRetryPolicy.isRetryableIOException(e)) {
                throw e;
            }
            throw new SQLException("Failed to decode Arrow response", e);
        } catch (Error e) {
            closeBatches(batches, e);
            closeAllocator(allocator, e);
            throw e;
        } catch (Exception e) {
            closeBatches(batches, e);
            closeAllocator(allocator, e);
            throw new SQLException("Failed to decode Arrow response", e);
        }

        try {
            List<QueryRowField> fields = ArrowResultPage.schemaToFields(schema);
            ResultPage page = new ArrowResultPage(allocator, batches, effectiveSettings(results));
            return new RestQueryResultPages.ResponsePayload(response.code(), response.headers(), results, page, fields);
        } catch (SQLException | RuntimeException | Error e) {
            closeBatches(batches, e);
            closeAllocator(allocator, e);
            throw e;
        }
    }

    private static void closeBatches(List<VectorSchemaRoot> batches, Throwable failure) {
        for (VectorSchemaRoot batch : batches) {
            try {
                ArrowResultPage.closeRoot(batch);
            }
            catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static void closeAllocator(BufferAllocator allocator, Throwable failure) {
        try {
            allocator.close();
        }
        catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    static long allocatedMemoryForTesting() {
        return RootAllocatorHolder.INSTANCE.getAllocatedMemory();
    }

    private static Map<String, String> effectiveSettings(QueryResults results) {
        Map<String, String> merged = new HashMap<>();
        if (results.getSession() != null && results.getSession().getSettings() != null) {
            merged.putAll(results.getSession().getSettings());
        }
        if (results.getSettings() != null) {
            merged.putAll(results.getSettings());
        }
        return merged;
    }

    /**
     * Arrow permits EOF as a stream terminator, but Databend always writes an explicit
     * EOS frame via StreamWriter.finish(). Reject EOF before that frame so a short
     * response cannot be accepted as a successful partial result. Arrow stops reading
     * after parsing EOS; this wrapper neither parses framing nor examines payload bytes.
     * HTTP framing can detect broken transfers, but not a short Arrow body enclosed in
     * an otherwise normally completed HTTP response.
     */
    private static final class EofRejectingChannel implements ReadableByteChannel {
        private final ReadableByteChannel delegate;

        private EofRejectingChannel(ReadableByteChannel delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            int count = delegate.read(dst);
            if (count < 0) {
                throw new TruncatedResponseException("Arrow response ended before an explicit end-of-stream frame");
            }
            return count;
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class RootAllocatorHolder {
        private static final RootAllocator INSTANCE = new RootAllocator(Long.MAX_VALUE);
    }
}
