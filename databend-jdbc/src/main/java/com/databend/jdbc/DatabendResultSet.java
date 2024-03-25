package com.databend.jdbc;

import com.databend.client.DatabendClient;
import com.databend.client.QueryResults;
import com.databend.client.QueryRowField;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.concurrent.GuardedBy;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DatabendResultSet extends AbstractDatabendResultSet
{

    private final String queryId;
    private final Statement statement;
    private final DatabendClient client;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean closeStatementOnClose;
    private DatabendResultSet(Statement statement, DatabendClient client, List<QueryRowField> schema, long maxRows) throws SQLException
    {
        super(Optional.of(requireNonNull(statement, "statement is null")), schema,
                new AsyncIterator<>(flatten(new ResultsPageIterator(client),maxRows), client));
        this.statement = statement;
        this.client = client;
        this.queryId = client.getResults().getQueryId();
    }

    static DatabendResultSet create(Statement statement, DatabendClient client, long maxRows)
            throws SQLException
    {
        requireNonNull(client, "client is null");
        List<QueryRowField> s = client.getResults().getSchema();
        return new DatabendResultSet(statement, client, s, maxRows);
    }

    private static <T> Iterator<T> flatten(Iterator<Iterable<T>> iterator, long maxRows)
    {
        Stream<T> stream = Streams.stream(iterator)
                .flatMap(Streams::stream);
        if (maxRows > 0) {
            stream = stream.limit(maxRows);
        }
        return stream.iterator();
    }

    public String getQueryId()
    {
        return queryId;
    }

    void setCloseStatementOnClose()
            throws SQLException
    {
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
            throws SQLException
    {
        boolean closeStatement;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            closeStatement = closeStatementOnClose;
        }

        ((AsyncIterator<?>) results).cancel();
        client.close();
        if (closeStatement) {
            statement.close();
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return closed;
    }

    static class AsyncIterator<T> extends AbstractIterator<T>
    {
        private static final int MAX_QUEUED_ROWS = 50_000;
        private static final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("Databend JDBC worker-%s").setDaemon(true).build());
        private final DatabendClient client;
        private final BlockingQueue<T> rowQueue;
        private final Semaphore semaphore = new Semaphore(0);
        private final Future<?> future;
        private volatile boolean cancelled;
        private volatile boolean finished;
        public AsyncIterator(Iterator<T> dataIterator, DatabendClient client)
        {
            this(dataIterator, client, Optional.empty());
        }
        @VisibleForTesting
        AsyncIterator(Iterator<T> dataIterator, DatabendClient client, Optional<BlockingQueue<T>> queue)
        {
            requireNonNull(dataIterator, "dataIterator is null");
            this.client = client;
            this.rowQueue = queue.orElseGet(() -> new ArrayBlockingQueue<>(MAX_QUEUED_ROWS));
            this.cancelled = false;
            this.finished = false;
            this.future = executorService.submit(() -> {
                try {
                    while (dataIterator.hasNext()) {
                        rowQueue.put(dataIterator.next());
                        semaphore.release();
                    }
                }
                catch (InterruptedException e) {
                    client.close();
                    rowQueue.clear();
                    throw new RuntimeException(new SQLException("ResultSet thread was interrupted", e));
                }
                finally {
                    semaphore.release();
                    finished = true;
                }
            });
        }

        public void cancel()
        {
            cancelled = true;
            future.cancel(true);
            // When thread interruption is mis-handled by underlying implementation of `client`, the thread which
            // is working for `future` may be blocked by `rowQueue.put` (`rowQueue` is full) and will never finish
            // its work. It is necessary to close `client` and drain `rowQueue` to avoid such leaks.
            client.close();
            rowQueue.clear();
        }

        @VisibleForTesting
        Future<?> getFuture()
        {
            return future;
        }
        @VisibleForTesting
        boolean isBackgroundThreadFinished()
        {
            return finished;
        }

        @Override
        protected T computeNext()
        {
            try {
                semaphore.acquire();
            }
            catch (InterruptedException e) {
                handleInterrupt(e);
            }
            if (rowQueue.isEmpty()) {
                try {
                    future.get();
                }
                catch (InterruptedException e) {
                    handleInterrupt(e);
                }
                catch (ExecutionException e) {
                    throwIfUnchecked(e.getCause());
                    throw new RuntimeException(e.getCause());
                }
                return endOfData();
            }
            return rowQueue.poll();
        }
        private void handleInterrupt(InterruptedException e)
        {
            cancel();
            Thread.currentThread().interrupt();
            throw new RuntimeException(new SQLException("Interrupted", e));
        }
    }

    private static class ResultsPageIterator extends AbstractIterator<Iterable<List<Object>>>
    {
        private final DatabendClient client;

        private ResultsPageIterator(DatabendClient client)
        {
            this.client = client;
        }

        @Override
        protected Iterable<List<Object>> computeNext()
        {
            while(client.hasNext()) {
                QueryResults results = client.getResults();
                Iterable<List<Object>> rows = results.getData();
                try {
                    client.next();
                } catch(RuntimeException e) {
                    throw new RuntimeException(e);
                }
                if (rows != null) {
                    return rows;
                }
            }
            // next uri is null, no more data
            QueryResults results = client.getResults();
            if (results.getError() != null) {
                throw new RuntimeException(resultsException(results));
            }
            return endOfData();
        }
    }
}
