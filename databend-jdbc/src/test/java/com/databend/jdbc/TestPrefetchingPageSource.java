package com.databend.jdbc;

import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.QueryResults;
import com.databend.jdbc.internal.query.QueryRowField;
import com.databend.jdbc.internal.query.ResultPage;
import com.databend.jdbc.internal.session.Capability;
import com.databend.jdbc.internal.session.QueryLiveness;
import com.databend.jdbc.internal.session.SessionState;
import com.vdurmont.semver4j.Semver;
import okhttp3.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Test(groups = "UNIT", timeOut = 15000)
public class TestPrefetchingPageSource {
    public void testCurrentPageDoesNotWaitForNextDownload() throws Exception {
        TestPage first = new TestPage(1);
        TestPage second = new TestPage(2);
        CountDownLatch advanceStarted = new CountDownLatch(1);
        CountDownLatch allowAdvance = new CountDownLatch(1);
        Pages pages = new Pages(first, second) {
            @Override
            public boolean advance() {
                advanceStarted.countDown();
                awaitUninterruptibly(allowAdvance);
                return super.advance();
            }
        };
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), worker);
        try {
            Future<ResultPage> current = caller.submit(source::nextPage);
            Assert.assertSame(current.get(5, TimeUnit.SECONDS), first);
            Assert.assertTrue(advanceStarted.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(first.getValue(0, 0), 1);
            Assert.assertEquals(second.closed.get(), 0);
            first.close();
            allowAdvance.countDown();
            Assert.assertSame(source.nextPage(), second);
            second.close();
            Assert.assertNull(source.nextPage());
            Assert.assertNull(source.nextPage());
        }
        finally {
            allowAdvance.countDown();
            source.close();
            stop(caller);
            stop(worker);
        }
        Assert.assertEquals(first.closed.get(), 1);
        Assert.assertEquals(second.closed.get(), 1);
    }

    public void testNextDownloadErrorDoesNotHideCurrentPage() throws Exception {
        TestPage first = new TestPage(1);
        IllegalStateException failure = new IllegalStateException("next page failed");
        Pages pages = new Pages(first) {
            @Override
            public boolean advance() {
                throw failure;
            }
        };
        ManualExecutor executor = new ManualExecutor();
        try (DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), executor)) {
            executor.runNext();
            Assert.assertSame(source.nextPage(), first);
            executor.runNext();
            Assert.assertEquals(first.getValue(0, 0), 1);
            SQLException actual = Assert.expectThrows(SQLException.class, source::nextPage);
            Assert.assertSame(actual.getCause(), failure);
            first.close();
        }
        Assert.assertEquals(first.closed.get(), 1);
    }

    public void testResultSetNextWrapsQueryFailureAsSQLException() throws Exception {
        TestPage first = new TestPage(1);
        DatabendQueryException failure = new DatabendQueryException("next page failed", new SQLException("decode failed"));
        Pages pages = new Pages(first) {
            @Override
            public boolean advance() {
                throw failure;
            }
        };
        Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
                new Class<?>[] {Statement.class}, (proxy, method, args) -> null);
        try (DatabendResultSet results = DatabendResultSet.create(statement, pages, 0, new Capability(new Semver("1.2.0")))) {
            Assert.assertTrue(results.next());
            SQLException actual = Assert.expectThrows(SQLException.class, results::next);
            Assert.assertSame(actual.getCause(), failure);
            Assert.assertEquals(actual.getCause().getCause().getMessage(), "decode failed");
        }
        Assert.assertEquals(first.closed.get(), 1);
    }

    public void testWorkerErrorIsNotWrappedAsSQLException() throws Exception {
        TestPage first = new TestPage(1);
        AssertionError failure = new AssertionError("fatal worker error");
        Pages pages = new Pages(first) {
            @Override
            public boolean advance() {
                throw failure;
            }
        };
        ManualExecutor executor = new ManualExecutor();
        try (DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), executor)) {
            executor.runNext();
            Assert.assertSame(source.nextPage(), first);
            executor.runNext();
            Assert.assertSame(Assert.expectThrows(AssertionError.class, source::nextPage), failure);
            first.close();
        }
    }

    public void testCloseBeforeQueuedFetchStarts() throws Exception {
        TestPage first = new TestPage(1);
        Pages pages = new Pages(first);
        QueryLiveness liveness = liveness();
        ManualExecutor executor = new ManualExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness, executor);
        source.close();
        source.close();
        executor.runNext();
        Assert.assertNull(source.nextPage());
        Assert.assertEquals(pages.takes.get(), 0);
        Assert.assertEquals(pages.closes.get(), 1);
        Assert.assertEquals(first.closed.get(), 1);
        Assert.assertTrue(liveness.stopped);
    }

    public void testCloseReleasesCompletedUnconsumedPage() throws Exception {
        TestPage first = new TestPage(1);
        Pages pages = new Pages(first);
        ManualExecutor executor = new ManualExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), executor);
        executor.runNext();
        Assert.assertNull(pages.getPage(), "prefetch must detach the page from the HTTP source");
        source.close();
        source.close();
        Assert.assertNull(source.nextPage());
        Assert.assertEquals(first.closed.get(), 1);
        Assert.assertEquals(pages.closes.get(), 1);
    }

    public void testCloseWhilePageIsBeingFetchedReleasesLatePageAndUnblocksReader() throws Exception {
        CountDownLatch pageTaken = new CountDownLatch(1);
        CountDownLatch allowReturn = new CountDownLatch(1);
        TestPage first = new TestPage(1) {
            @Override
            public int getRowCount() {
                pageTaken.countDown();
                // Simulate a decode/HTTP operation that completes despite interruption.
                awaitUninterruptibly(allowReturn);
                return super.getRowCount();
            }
        };
        Pages pages = new Pages(first);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), worker);
        try {
            Assert.assertTrue(pageTaken.await(5, TimeUnit.SECONDS));
            Future<ResultPage> waiting = caller.submit(source::nextPage);
            source.close();
            Assert.assertNull(waiting.get(5, TimeUnit.SECONDS));
            Assert.assertEquals(first.closed.get(), 0, "worker still owns the detached page");
            allowReturn.countDown();
            worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
            Assert.assertEquals(first.closed.get(), 1, "cancelled Future must not discard a live page");
            Assert.assertEquals(pages.closes.get(), 1);
        }
        finally {
            allowReturn.countDown();
            source.close();
            stop(caller);
            stop(worker);
        }
    }

    public void testInterruptedReaderCancelsPrefetchAndPreservesInterrupt() throws Exception {
        CountDownLatch pageTaken = new CountDownLatch(1);
        CountDownLatch allowReturn = new CountDownLatch(1);
        TestPage first = new TestPage(1) {
            @Override
            public int getRowCount() {
                pageTaken.countDown();
                awaitUninterruptibly(allowReturn);
                return super.getRowCount();
            }
        };
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(new Pages(first), liveness(), worker);
        try {
            Assert.assertTrue(pageTaken.await(5, TimeUnit.SECONDS));
            caller.submit(() -> {
                Thread.currentThread().interrupt();
                try {
                    SQLException failure = Assert.expectThrows(SQLException.class, source::nextPage);
                    Assert.assertTrue(failure.getCause() instanceof InterruptedException);
                    Assert.assertTrue(Thread.currentThread().isInterrupted());
                }
                finally {
                    Thread.interrupted();
                }
            }).get(5, TimeUnit.SECONDS);
            allowReturn.countDown();
            worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
            Assert.assertEquals(first.closed.get(), 1);
            Assert.assertNull(source.nextPage());
        }
        finally {
            allowReturn.countDown();
            source.close();
            stop(caller);
            stop(worker);
        }
    }

    public void testCloseCannotMissTaskWhileItIsBeingScheduled() throws Exception {
        TestPage first = new TestPage(1);
        Pages pages = new Pages(first);
        CountDownLatch scheduling = new CountDownLatch(1);
        CountDownLatch allowSchedule = new CountDownLatch(1);
        ManualExecutor executor = new ManualExecutor() {
            private int submissions;

            @Override
            public void execute(Runnable task) {
                if (++submissions == 2) {
                    scheduling.countDown();
                    awaitUninterruptibly(allowSchedule);
                }
                super.execute(task);
            }
        };
        ExecutorService caller = Executors.newSingleThreadExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), executor);
        try {
            executor.runNext();
            Future<ResultPage> current = caller.submit(source::nextPage);
            Assert.assertTrue(scheduling.await(5, TimeUnit.SECONDS));
            source.close();
            allowSchedule.countDown();
            Assert.assertSame(current.get(5, TimeUnit.SECONDS), first);
            Assert.assertEquals(first.getValue(0, 0), 1, "close must not release a page already handed to the caller");
            executor.runNext();
            Assert.assertEquals(pages.takes.get(), 1, "cancelled queued task must not fetch another page");
            first.close();
        }
        finally {
            allowSchedule.countDown();
            source.close();
            stop(caller);
        }
    }

    public void testMaxRowsThenCloseReleasesPrefetchedPage() throws Exception {
        TestPage first = new TestPage(1);
        TestPage second = new TestPage(2);
        Pages pages = new Pages(first, second);
        ManualExecutor executor = new ManualExecutor();
        DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), executor);
        PagedResultCursor cursor = new PagedResultCursor(source, 1);
        try {
            executor.runNext();
            Assert.assertTrue(cursor.next());
            Assert.assertEquals(cursor.getValue(0), 1);
            executor.runNext();
            Assert.assertFalse(cursor.next());
            cursor.close();
            Assert.assertEquals(first.closed.get(), 1);
            Assert.assertEquals(second.closed.get(), 1);
            Assert.assertEquals(pages.closes.get(), 1);
        }
        finally {
            cursor.close();
        }
    }

    public void testRejectedNextTaskReleasesCurrentPage() throws Exception {
        TestPage first = new TestPage(1);
        Pages pages = new Pages(first);
        ManualExecutor executor = new ManualExecutor();
        try (DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(pages, liveness(), executor)) {
            executor.runNext();
            executor.shutdown();
            SQLException failure = Assert.expectThrows(SQLException.class, source::nextPage);
            Assert.assertTrue(failure.getCause() instanceof RejectedExecutionException);
            Assert.assertEquals(first.closed.get(), 1);
            Assert.assertEquals(pages.closes.get(), 1);
            Assert.assertNull(source.nextPage());
        }
    }

    public void testEmptyPagesAdvanceAndUpdateLiveness() throws Exception {
        TestPage empty = new TestPage(1) {
            @Override
            public int getRowCount() {
                return 0;
            }
        };
        QueryLiveness liveness = liveness();
        ManualExecutor executor = new ManualExecutor();
        try (DatabendResultSet.PrefetchingPageSource source = new DatabendResultSet.PrefetchingPageSource(new Pages(empty), liveness, executor)) {
            executor.runNext();
            Assert.assertNull(source.nextPage());
            Assert.assertNull(source.nextPage());
            Assert.assertEquals(empty.closed.get(), 1);
            Assert.assertTrue(liveness.stopped);
            Assert.assertTrue(liveness.lastRequestTime.get() > 0);
        }
    }

    private static QueryLiveness liveness() {
        return new QueryLiveness("qid", "node", new AtomicLong(0), 30L, false);
    }

    private static void stop(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "executor did not stop");
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (!latch.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("test gate was not released");
                    }
                    return;
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class TestPage implements ResultPage {
        private final int value;
        final AtomicInteger closed = new AtomicInteger();

        TestPage(int value) {
            this.value = value;
        }

        @Override
        public int getRowCount() {
            return 1;
        }

        @Override
        public Object getValue(int row, int column) {
            Assert.assertEquals(closed.get(), 0, "consumer's page was closed by prefetch");
            return value;
        }

        @Override
        public void close() {
            Assert.assertEquals(closed.incrementAndGet(), 1, "page was closed more than once");
        }
    }

    private static class Pages implements QueryResultPages {
        private final List<TestPage> pages;
        private int index;
        private boolean taken;
        private boolean closed;
        final AtomicInteger takes = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();

        Pages(TestPage... pages) {
            this.pages = Arrays.asList(pages);
        }

        @Override
        public synchronized ResultPage getPage() {
            return hasNext() && !taken ? pages.get(index) : null;
        }

        @Override
        public synchronized ResultPage takePage() {
            takes.incrementAndGet();
            ResultPage page = getPage();
            taken = true;
            return page;
        }

        @Override
        public synchronized boolean advance() {
            if (!closed) {
                index++;
                taken = false;
            }
            return hasNext();
        }

        @Override
        public synchronized boolean hasNext() {
            return !closed && index < pages.size();
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                ResultPage page = getPage();
                closed = true;
                closes.incrementAndGet();
                if (page != null) {
                    page.close();
                }
            }
        }

        @Override
        public String getQuery() {
            return "select 1";
        }

        @Override
        public SessionState getSession() {
            return SessionState.createDefault();
        }

        @Override
        public String getNodeID() {
            return "node";
        }

        @Override
        public QueryResults getResults() {
            return new QueryResults("qid", "node", null, SessionState.createDefault(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), "Running",
                    null, null, null, 30, null, null, URI.create("/v1/query/final"), null);
        }

        @Override
        public List<QueryRowField> getSchema() {
            return Collections.emptyList();
        }

        @Override
        public boolean execute(Request request) {
            return false;
        }
    }

    private static class ManualExecutor extends AbstractExecutorService {
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private boolean shutdown;

        void runNext() {
            Runnable task = tasks.remove();
            task.run();
        }

        @Override
        public void execute(Runnable task) {
            if (shutdown) {
                throw new RejectedExecutionException("test executor is shut down");
            }
            tasks.add(task);
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown();
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }
    }
}
