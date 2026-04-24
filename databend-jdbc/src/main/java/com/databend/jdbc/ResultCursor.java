package com.databend.jdbc;

import com.databend.jdbc.internal.query.ResultPage;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

interface ResultCursor {
    boolean next() throws SQLException;

    Object getValue(int columnIndex) throws SQLException;

    default void close() throws SQLException {
    }
}

interface ResultPageSource extends AutoCloseable {
    ResultPage nextPage() throws SQLException;

    @Override
    default void close() {
    }
}

final class IteratorResultCursor implements ResultCursor {
    private final Iterator<List<Object>> rows;
    private List<Object> currentRow;

    IteratorResultCursor(Iterator<List<Object>> rows) {
        this.rows = rows;
    }

    @Override
    public boolean next() {
        if (!rows.hasNext()) {
            currentRow = null;
            return false;
        }
        currentRow = rows.next();
        return true;
    }

    @Override
    public Object getValue(int columnIndex) throws SQLException {
        if (currentRow == null) {
            throw new SQLException("Not on a valid row");
        }
        return currentRow.get(columnIndex);
    }
}

final class PagedResultCursor implements ResultCursor {
    private final ResultPageSource pageSource;
    private final long maxRows;
    private ResultPage currentPage;
    private int currentRowInPage = -1;
    private long rowsRead;

    PagedResultCursor(ResultPageSource pageSource, long maxRows) {
        this.pageSource = pageSource;
        this.maxRows = maxRows;
    }

    @Override
    public boolean next() throws SQLException {
        if (maxRows > 0 && rowsRead >= maxRows) {
            closeCurrentPage();
            return false;
        }
        while (true) {
            if (currentPage != null && currentRowInPage + 1 < currentPage.getRowCount()) {
                currentRowInPage++;
                rowsRead++;
                return true;
            }
            closeCurrentPage();
            currentPage = pageSource.nextPage();
            if (currentPage == null) {
                return false;
            }
            currentRowInPage = -1;
        }
    }

    @Override
    public Object getValue(int columnIndex) throws SQLException {
        if (currentPage == null || currentRowInPage < 0) {
            throw new SQLException("Not on a valid row");
        }
        return currentPage.getValue(currentRowInPage, columnIndex);
    }

    @Override
    public void close() throws SQLException {
        try {
            closeCurrentPage();
        }
        finally {
            pageSource.close();
        }
    }

    private void closeCurrentPage() {
        if (currentPage != null) {
            currentPage.close();
            currentPage = null;
            currentRowInPage = -1;
        }
    }
}
