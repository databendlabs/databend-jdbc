package com.databend.jdbc;

import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.QueryResults;
import com.databend.jdbc.internal.query.QueryRowField;
import com.databend.jdbc.internal.query.ResultPage;
import com.databend.jdbc.internal.session.Capability;
import com.databend.jdbc.internal.session.QueryLiveness;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.concurrent.GuardedBy;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DatabendResultSet extends AbstractDatabendResultSet {
    private final Statement statement;
    // Writes remain guarded by this; unsynchronized reads rely on volatile visibility.
    private volatile boolean closed;
    @GuardedBy("this")
    private boolean closeStatementOnClose;

    private final QueryLiveness liveness;

    private DatabendResultSet(Statement statement, QueryResultPages queryPages, List<QueryRowField> schema, Map<String, String> resultSetting, long maxRows, QueryLiveness liveness) {
        this(statement, queryPages, schema, resultSetting, maxRows, liveness, new PrefetchingPageSource(queryPages, liveness));
    }

    private DatabendResultSet(Statement statement, QueryResultPages queryPages, List<QueryRowField> schema, Map<String, String> resultSetting, long maxRows, QueryLiveness liveness, PrefetchingPageSource pageSource) {
        super(Optional.of(requireNonNull(statement, "statement is null")), schema,
                new PagedResultCursor(pageSource, maxRows), resultSetting, queryPages.getResults().getQueryId());
        this.statement = statement;
        this.liveness = liveness;
    }

    static DatabendResultSet create(Statement statement, QueryResultPages queryPages, long maxRows, Capability capability)
            throws SQLException {
        requireNonNull(queryPages, "queryPages is null");
        List<QueryRowField> schema = queryPages.getSchema();
        if (schema == null) {
            schema = queryPages.getResults().getSchema();
        }
        Map<String, String> resultSettings = effectiveSettings(queryPages.getResults());
        AtomicLong lastRequestTime = new AtomicLong(System.currentTimeMillis());
        QueryResults results = queryPages.getResults();
        QueryLiveness liveness = new QueryLiveness(results.getQueryId(), queryPages.getNodeID(), lastRequestTime, results.getResultTimeoutSecs(), capability.heartBeat());
        return new DatabendResultSet(statement, queryPages, schema, resultSettings, maxRows, liveness);
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

        results.close();
        if (closeStatement) {
            statement.close();
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException {
        return closed;
    }

    static class PrefetchingPageSource implements ResultPageSource {
        private static final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("Databend JDBC worker-%s").setDaemon(true).build());
        private final QueryResultPages queryPages;
        private final QueryLiveness liveness;
        private final ExecutorService executor;
        private final Object stateLock = new Object();
        @GuardedBy("stateLock")
        private ResultPage prefetchedPage;
        private volatile Future<ResultPage> inFlight;
        private volatile boolean cancelled;
        private boolean finished;

        PrefetchingPageSource(QueryResultPages queryPages, QueryLiveness liveness) {
            this(queryPages, liveness, executorService);
        }

        @VisibleForTesting
        PrefetchingPageSource(QueryResultPages queryPages, QueryLiveness liveness, ExecutorService executor) {
            this.queryPages = requireNonNull(queryPages, "queryPages is null");
            this.liveness = requireNonNull(liveness, "liveness is null");
            this.executor = requireNonNull(executor, "executor is null");
            scheduleFetch(false);
        }

        @Override
        public void close() {
            Future<ResultPage> future;
            ResultPage page;
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                cancelled = true;
                liveness.stopped = true;
                future = inFlight;
                page = prefetchedPage;
                prefetchedPage = null;
            }
            if (future != null) {
                future.cancel(true);
            }
            closeQuietly(page);
            queryPages.close();
        }

        @Override
        public ResultPage nextPage() throws SQLException {
            if (cancelled || finished) {
                return null;
            }

            ResultPage page = awaitPrefetchedPage();
            synchronized (stateLock) {
                if (cancelled) {
                    return null;
                }
                if (page == null) {
                    finished = true;
                    return null;
                }
                // Ownership moves to the cursor before the next download starts.
                prefetchedPage = null;
            }

            try {
                scheduleFetch(true);
            }
            catch (RuntimeException | Error e) {
                closeQuietly(page);
                close();
                throw e;
            }
            return page;
        }

        private void scheduleFetch(boolean advanceFirst) {
            FutureTask<ResultPage> task = new FutureTask<>(() -> fetchNextPage(advanceFirst));
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                // Publish before execution, so close() cannot miss a newly scheduled task.
                inFlight = task;
            }
            executor.execute(task);
        }

        private ResultPage awaitPrefetchedPage() throws SQLException {
            Future<ResultPage> future = inFlight;
            if (future == null) {
                return null;
            }
            try {
                return future.get();
            }
            catch (InterruptedException e) {
                handleInterrupt(e);
                return null;
            }
            catch (CancellationException e) {
                if (cancelled) {
                    return null;
                }
                throw new SQLException("Prefetch cancelled", e);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException) {
                    throw (SQLException) cause;
                }
                throwIfUnchecked(cause);
                throw new SQLException("Failed to fetch result page", cause);
            }
        }

        private ResultPage fetchNextPage(boolean advanceFirst) throws SQLException {
            if (cancelled) {
                return null;
            }
            if (advanceFirst && !advance()) {
                return finish();
            }
            while (!cancelled && queryPages.hasNext()) {
                ResultPage page = queryPages.takePage();
                if (page != null && page.getRowCount() > 0) {
                    synchronized (stateLock) {
                        if (!cancelled) {
                            // A cancelled Future discards its return value. Keep ownership
                            // separately so close() can still release this page.
                            prefetchedPage = page;
                            return page;
                        }
                    }
                    closeQuietly(page);
                    return null;
                }
                closeQuietly(page);
                if (!advance()) {
                    break;
                }
            }
            return finish();
        }

        private boolean advance() {
            if (cancelled) {
                return false;
            }
            boolean hasNext = queryPages.advance();
            liveness.lastRequestTime.set(System.currentTimeMillis());
            return hasNext;
        }

        private ResultPage finish() throws SQLException {
            liveness.stopped = true;
            QueryResults results = queryPages.getResults();
            if (results != null && results.getError() != null) {
                throw resultsException(results, queryPages.getQuery());
            }
            return null;
        }

        private static void closeQuietly(ResultPage page) {
            if (page != null) {
                try {
                    page.close();
                }
                catch (Exception ignored) {
                }
            }
        }

        private void handleInterrupt(InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw new RuntimeException(new SQLException("Interrupted", e));
        }
    }
}
