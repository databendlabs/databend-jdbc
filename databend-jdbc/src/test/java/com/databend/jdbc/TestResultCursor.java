package com.databend.jdbc;

import com.databend.jdbc.internal.error.QueryError;
import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.QueryResults;
import com.databend.jdbc.internal.query.QueryRowField;
import com.databend.jdbc.internal.query.ResultPage;
import com.databend.jdbc.internal.session.QueryLiveness;
import com.databend.jdbc.internal.session.SessionState;
import com.google.common.util.concurrent.MoreExecutors;
import okhttp3.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestResultCursor {
    @Test(groups = {"UNIT"})
    public void testPagedResultCursorUsesPagesAndClosesThem() throws SQLException {
        AtomicInteger closedPages = new AtomicInteger();
        ResultPage page1 = new FakePage(Arrays.asList(Collections.singletonList(1), Collections.singletonList(2)), closedPages);
        ResultPage page2 = new FakePage(Arrays.asList(Collections.singletonList(3), Collections.singletonList(4)), closedPages);
        AtomicInteger closedSources = new AtomicInteger();

        PagedResultCursor cursor = new PagedResultCursor(new FakePageSource(Arrays.asList(page1, page2), closedSources), 3);

        Assert.assertTrue(cursor.next());
        Assert.assertEquals(cursor.getValue(0), 1);
        Assert.assertTrue(cursor.next());
        Assert.assertEquals(cursor.getValue(0), 2);
        Assert.assertTrue(cursor.next());
        Assert.assertEquals(cursor.getValue(0), 3);
        Assert.assertFalse(cursor.next());
        cursor.close();
        Assert.assertEquals(closedPages.get(), 2);
        Assert.assertEquals(closedSources.get(), 1);
    }

    @Test(groups = {"UNIT"})
    public void testPrefetchingPageSourceReturnsPagesInOrderAndSkipsEmptyPages() throws SQLException {
        AtomicInteger closedPages = new AtomicInteger();
        FakePage empty1 = new FakePage(Collections.emptyList(), closedPages);
        FakePage page1 = new FakePage(Arrays.asList(Collections.singletonList(1), Collections.singletonList(2)), closedPages);
        FakePage empty2 = new FakePage(Collections.emptyList(), closedPages);
        FakePage page2 = new FakePage(Collections.singletonList(Collections.singletonList(3)), closedPages);

        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        try {
            DatabendResultSet.PrefetchingPageSource pageSource = new DatabendResultSet.PrefetchingPageSource(
                    new FakeQueryResultPages(Arrays.asList(empty1, page1, empty2, page2), successResults(), successResults()),
                    newLiveness(),
                    executor);

            Assert.assertSame(pageSource.nextPage(), page1);
            Assert.assertSame(pageSource.nextPage(), page2);
            Assert.assertNull(pageSource.nextPage());
            Assert.assertEquals(closedPages.get(), 2);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test(groups = {"UNIT"})
    public void testPrefetchingPageSourceCloseClosesPrefetchedPage() throws SQLException {
        AtomicInteger closedPages = new AtomicInteger();
        FakePage page1 = new FakePage(Collections.singletonList(Collections.singletonList(1)), closedPages);
        FakePage page2 = new FakePage(Collections.singletonList(Collections.singletonList(2)), closedPages);

        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        try {
            DatabendResultSet.PrefetchingPageSource pageSource = new DatabendResultSet.PrefetchingPageSource(
                    new FakeQueryResultPages(Arrays.asList(page1, page2), successResults(), successResults()),
                    newLiveness(),
                    executor);

            Assert.assertSame(pageSource.nextPage(), page1);
            pageSource.close();
            Assert.assertEquals(closedPages.get(), 1);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test(groups = {"UNIT"})
    public void testPrefetchingPageSourcePropagatesTerminalError() throws SQLException {
        AtomicInteger closedPages = new AtomicInteger();
        FakePage page1 = new FakePage(Collections.singletonList(Collections.singletonList(1)), closedPages);
        QueryResults errorResults = new QueryResults(
                "qid",
                "node",
                null,
                SessionState.createDefault(),
                Collections.<QueryRowField>emptyList(),
                Collections.<List<String>>emptyList(),
                Collections.<String, String>emptyMap(),
                "Failed",
                QueryError.builder().setCode(500).setMessage("boom").build(),
                null,
                null,
                30,
                null,
                null,
                URI.create("/v1/query/final"),
                null);

        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        try {
            DatabendResultSet.PrefetchingPageSource pageSource = new DatabendResultSet.PrefetchingPageSource(
                    new FakeQueryResultPages(Collections.singletonList(page1), successResults(), errorResults),
                    newLiveness(),
                    executor);

            Assert.assertSame(pageSource.nextPage(), page1);
            SQLException exception = Assert.expectThrows(SQLException.class, pageSource::nextPage);
            Assert.assertTrue(exception.getMessage().contains("Query failed"));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test(groups = {"UNIT"})
    public void testPagedResultCursorPropagatesPageSourceException() throws SQLException {
        PagedResultCursor cursor = new PagedResultCursor(new FailingPageSource(new SQLException("boom")), 0);
        SQLException exception = Assert.expectThrows(SQLException.class, cursor::next);
        Assert.assertEquals(exception.getMessage(), "boom");
    }

    private static QueryLiveness newLiveness() {
        return new QueryLiveness("qid", "node", new AtomicLong(System.currentTimeMillis()), 30L, false);
    }

    private static QueryResults successResults() {
        return new QueryResults(
                "qid",
                "node",
                null,
                SessionState.createDefault(),
                Collections.<QueryRowField>emptyList(),
                Collections.<List<String>>emptyList(),
                Collections.<String, String>emptyMap(),
                "Running",
                null,
                null,
                null,
                30,
                null,
                null,
                URI.create("/v1/query/next"),
                null);
    }

    private static final class FakePage implements ResultPage {
        private final List<List<Object>> rows;
        private final AtomicInteger closedPages;

        private FakePage(List<List<Object>> rows, AtomicInteger closedPages) {
            this.rows = rows;
            this.closedPages = closedPages;
        }

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public Object getValue(int rowIndex, int columnIndex) {
            return rows.get(rowIndex).get(columnIndex);
        }

        @Override
        public void close() {
            closedPages.incrementAndGet();
        }
    }

    private static final class FakePageSource implements ResultPageSource {
        private final List<ResultPage> pages;
        private final AtomicInteger closedSources;
        private int index;

        private FakePageSource(List<ResultPage> pages, AtomicInteger closedSources) {
            this.pages = pages;
            this.closedSources = closedSources;
        }

        @Override
        public ResultPage nextPage() {
            if (index >= pages.size()) {
                return null;
            }
            return pages.get(index++);
        }

        @Override
        public void close() {
            closedSources.incrementAndGet();
        }
    }

    private static final class FailingPageSource implements ResultPageSource {
        private final SQLException exception;

        private FailingPageSource(SQLException exception) {
            this.exception = exception;
        }

        @Override
        public ResultPage nextPage() throws SQLException {
            throw exception;
        }
    }

    private static final class FakeQueryResultPages implements QueryResultPages {
        private final List<ResultPage> pages;
        private final QueryResults currentResults;
        private final QueryResults terminalResults;
        private int index;
        private boolean closed;

        private FakeQueryResultPages(List<ResultPage> pages, QueryResults currentResults, QueryResults terminalResults) {
            this.pages = pages;
            this.currentResults = currentResults;
            this.terminalResults = terminalResults;
        }

        @Override
        public String getQuery() {
            return "select 1";
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public SessionState getSession() {
            return SessionState.createDefault();
        }

        @Override
        public String getNodeID() {
            return "node";
        }

        public Map<String, String> getAdditionalHeaders() {
            return Collections.emptyMap();
        }

        @Override
        public QueryResults getResults() {
            if (index >= pages.size()) {
                return terminalResults;
            }
            return currentResults;
        }

        @Override
        public List<QueryRowField> getSchema() {
            return Collections.emptyList();
        }

        @Override
        public ResultPage getPage() {
            return hasNext() ? pages.get(index) : null;
        }

        @Override
        public boolean execute(Request request) {
            return false;
        }

        @Override
        public boolean advance() {
            if (index < pages.size()) {
                index++;
            }
            return hasNext();
        }

        @Override
        public boolean hasNext() {
            return !closed && index < pages.size();
        }
    }
}
