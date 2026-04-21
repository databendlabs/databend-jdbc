package com.databend.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.QueryResults;
import com.databend.jdbc.internal.query.QueryRowField;
import com.databend.jdbc.internal.session.Capability;
import com.databend.jdbc.internal.session.QueryLiveness;

import javax.annotation.concurrent.GuardedBy;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DatabendResultSet extends AbstractDatabendResultSet {
    private final Statement statement;
    private final QueryResultPages queryPages;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean closeStatementOnClose;

    private final QueryLiveness liveness;

    private DatabendResultSet(Statement statement, QueryResultPages queryPages, List<QueryRowField> schema, Map<String, String> resultSetting, long maxRows, QueryLiveness liveness) {
        super(Optional.of(requireNonNull(statement, "statement is null")), schema,
                new AsyncIterator<>(flatten(new ResultsPageIterator(queryPages, liveness), maxRows), queryPages), resultSetting, queryPages.getResults().getQueryId());
        this.statement = statement;
        this.queryPages = queryPages;
        this.liveness = liveness;
    }

    static DatabendResultSet create(Statement statement, QueryResultPages queryPages, long maxRows, Capability capability)
            throws SQLException {
        requireNonNull(queryPages, "queryPages is null");
        List<QueryRowField> s = queryPages.getResults().getSchema();
        Map<String, String> resultSettings = queryPages.getResults().getSettings();
        // Fallback: if top-level settings has no timezone, try session.settings
        if (resultSettings == null || !resultSettings.containsKey("timezone")) {
            if (queryPages.getResults().getSession() != null && queryPages.getResults().getSession().getSettings() != null) {
                Map<String, String> sessionSettings = queryPages.getResults().getSession().getSettings();
                if (sessionSettings.containsKey("timezone")) {
                    if (resultSettings == null) {
                        resultSettings = sessionSettings;
                    } else {
                        resultSettings = new java.util.HashMap<>(resultSettings);
                        resultSettings.put("timezone", sessionSettings.get("timezone"));
                    }
                }
            }
        }
        AtomicLong lastRequestTime = new AtomicLong(System.currentTimeMillis());
        QueryResults r = queryPages.getResults();
        QueryLiveness liveness = new QueryLiveness(r.getQueryId(), queryPages.getNodeID(), lastRequestTime,  r.getResultTimeoutSecs(), capability.heartBeat());
        return new DatabendResultSet(statement, queryPages, s, resultSettings, maxRows, liveness);
    }

    private static <T> Iterator<T> flatten(Iterator<Iterable<T>> iterator, long maxRows) {
        Stream<T> stream = Streams.stream(iterator)
                .flatMap(Streams::stream);
        if (maxRows > 0) {
            stream = stream.limit(maxRows);
        }
        return stream.iterator();
    }


    QueryLiveness getLiveness() {
        if (closed) {
	        return null;
        }
        return liveness;
    }

    void setCloseStatementOnClose()
            throws SQLException {
        boolean alreadyClosed;
        synchronized (this) {
            alreadyClosed = closed;
            if (!alreadyClosed) {
                closeStatementOnClose = true;
            }
        }
        if (alreadyClosed) {
            statement.close();
        }
    }

    @Override
    public void close()
            throws SQLException {
        boolean closeStatement;
        synchronized (this) {
            if (closed) {
                return;
            }
            liveness.stopped = true;
            closed = true;
            closeStatement = closeStatementOnClose;
        }

        ((AsyncIterator<?>) results).cancel();
        queryPages.close();
        if (closeStatement) {
            statement.close();
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException {
        return closed;
    }

    static class AsyncIterator<T> extends AbstractIterator<T> {
        private static final int MAX_QUEUED_ROWS = 50_000;
        private static final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("Databend JDBC worker-%s").setDaemon(true).build());
        private final QueryResultPages queryPages;
        private final BlockingQueue<T> rowQueue;
        private final Semaphore semaphore = new Semaphore(0);
        private final Future<?> future;

        public AsyncIterator(Iterator<T> dataIterator, QueryResultPages queryPages) {
            this(dataIterator, queryPages, Optional.empty());
        }

        @VisibleForTesting
        AsyncIterator(Iterator<T> dataIterator, QueryResultPages queryPages, Optional<BlockingQueue<T>> queue) {
            requireNonNull(dataIterator, "dataIterator is null");
            this.queryPages = queryPages;
            this.rowQueue = queue.orElseGet(() -> new ArrayBlockingQueue<>(MAX_QUEUED_ROWS));
            this.future = executorService.submit(() -> {
                try {
                    while (dataIterator.hasNext()) {
                        rowQueue.put(dataIterator.next());
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    queryPages.close();
                    rowQueue.clear();
                    throw new RuntimeException(new SQLException("ResultSet thread was interrupted", e));
                } finally {
                    semaphore.release();
                }
            });
        }

        public void cancel() {
            future.cancel(true);
            // When thread interruption is mis-handled by underlying implementation of `queryPages`, the thread which
            // is working for `future` may be blocked by `rowQueue.put` (`rowQueue` is full) and will never finish
            // its work. It is necessary to close `queryPages` and drain `rowQueue` to avoid such leaks.
            queryPages.close();
            rowQueue.clear();
        }

        @Override
        protected T computeNext() {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                handleInterrupt(e);
            }
            if (rowQueue.isEmpty()) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    handleInterrupt(e);
                } catch (ExecutionException e) {
                    throwIfUnchecked(e.getCause());
                    throw new RuntimeException(e.getCause());
                }
                return endOfData();
            }
            return rowQueue.poll();
        }

        private void handleInterrupt(InterruptedException e) {
            cancel();
            Thread.currentThread().interrupt();
            throw new RuntimeException(new SQLException("Interrupted", e));
        }
    }

    private static class ResultsPageIterator extends AbstractIterator<Iterable<List<Object>>> {
        private final QueryResultPages queryPages;
        private final QueryLiveness liveness;

        private ResultsPageIterator(QueryResultPages queryPages, QueryLiveness liveness) {
            this.queryPages = queryPages;
            this.liveness = liveness;
        }

        // AsyncIterator will call this and put rows to queue with MAX_QUEUED_ROWS=5000
        @Override
        protected Iterable<List<Object>> computeNext() {
            while (queryPages.hasNext()) {
                QueryResults results = queryPages.getResults();
                List<List<Object>> rows = results.getData();
                queryPages.advance();
                liveness.lastRequestTime.set(System.currentTimeMillis());
                if (rows != null) {
                    return rows;
                }
            }
            liveness.stopped = true;
            // next uri is null, no more data
            QueryResults results = queryPages.getResults();
            if (results.getError() != null) {
                throw new RuntimeException(resultsException(results, queryPages.getQuery()));
            }
            return endOfData();
        }
    }
}
