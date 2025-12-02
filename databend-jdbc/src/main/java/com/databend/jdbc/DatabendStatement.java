package com.databend.jdbc;

import com.databend.client.DatabendClient;
import com.databend.client.QueryResults;
import com.databend.client.StageAttachment;
import com.databend.jdbc.annotation.NotImplemented;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.databend.jdbc.AbstractDatabendResultSet.resultsException;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DatabendStatement implements Statement {
    private final AtomicReference<DatabendConnection> connection;
    private final Consumer<DatabendStatement> onClose;
    private int currentUpdateCount = -1;
    private final AtomicReference<DatabendResultSet> currentResult = new AtomicReference<>();
    private final AtomicReference<DatabendClient> executingClient = new AtomicReference<>();
    private final AtomicLong maxRows = new AtomicLong();
    private final AtomicBoolean closeOnCompletion = new AtomicBoolean();

    DatabendStatement(DatabendConnection connection, Consumer<DatabendStatement> onClose) {
        this.connection = new AtomicReference<>(requireNonNull(connection, "connection is null"));
        this.onClose = requireNonNull(onClose, "onClose is null");
    }

    @Override
    public ResultSet executeQuery(String s)
            throws SQLException {
        execute(s);
        return currentResult.get();
    }

    @Override
    public int executeUpdate(String s)
            throws SQLException {
        return 0;
    }

    @Override
    public void close()
            throws SQLException {
        DatabendConnection connection = this.connection.getAndSet(null);
        if (connection == null) {
            return;
        }
        onClose.accept(this);
        DatabendClient client = executingClient.get();
        if (client != null) {
            client.close();
        }
        closeResultSet();
    }

    @Override
    public int getMaxFieldSize()
            throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int i)
            throws SQLException {

    }

    @Override
    public int getMaxRows()
            throws SQLException {
        long result = maxRows.get();
        if (result > Integer.MAX_VALUE) {
            throw new SQLException("Max rows exceeds limit of 2147483647");
        }
        return toIntExact(result);
    }

    @Override
    public void setMaxRows(int i)
            throws SQLException {
        if (i < 0) {
            throw new SQLException("Max rows must be greater than or equal to zero");
        }
        maxRows.set(i);
    }

    @Override
    public void setEscapeProcessing(boolean b)
            throws SQLException {
        checkOpen();
    }

    @Override
    public int getQueryTimeout()
            throws SQLException {
        return 3000;
    }

    @Override
    public void setQueryTimeout(int i)
            throws SQLException {
    }

    @Override
    public void cancel()
            throws SQLException {
        checkOpen();
        DatabendClient client = executingClient.get();
        if (client != null) {
            client.close();
        }
        closeResultSet();
    }

    private void closeResultSet()
            throws SQLException {
        ResultSet resultSet = currentResult.getAndSet(null);
        if (resultSet != null) {
            resultSet.close();
        }
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException {

    }

    @Override
    public void setCursorName(String s)
            throws SQLException {
        checkOpen();
    }

    @Override
    public boolean execute(String s)
            throws SQLException {
        boolean result = internalExecute(s, null);
        if (!result) {
            currentUpdateCount = -1;
        }
        return result;
    }

    private void clearCurrentResults() throws SQLException {
        ResultSet old = currentResult.getAndSet(null);
        if (old != null) {
            old.close();
        }
    }

    final boolean internalExecute(String sql, StageAttachment attachment) throws SQLException {
        clearCurrentResults();
        checkOpen();
        DatabendClient client = null;
        DatabendResultSet resultSet = null;

        try {
            if (attachment == null) {
                client = connection().startQuery(sql);
            } else {
                client = connection().startQuery(sql, attachment);
            }
            if (!client.hasNext()) {
                if (client.getResults() != null && client.getResults().getError() != null) {
                    throw resultsException(client.getResults(), sql);
                }
            }
            executingClient.set(client);
            while (client.hasNext()) {
                QueryResults results = client.getResults();
                List<List<Object>> data = results.getData();
                if (data == null || data.isEmpty()) {
                    client.advance();
                } else {
                    break;
                }
            }
            resultSet = DatabendResultSet.create(this, client, maxRows.get(), connection().getServerCapability());
            currentResult.set(resultSet);
            if (isQueryStatement(sql)) {
                // Always -1 when returning a ResultSet with query statement
                currentUpdateCount = -1;
            } else {
                QueryResults results = client.getResults();
                if (sql.toLowerCase().startsWith("update") || sql.toLowerCase().startsWith("delete")) {
                    List<List<Object>> data = results.getData();
                    if (data != null && !data.isEmpty() && data.get(0) != null && !data.get(0).isEmpty()) {
                        Object updateCount = data.get(0).get(0);
                        if (updateCount instanceof Number) {
                            currentUpdateCount = ((Number) updateCount).intValue();
                        } else {
                            // if not found, use writeProgress.rows
                            currentUpdateCount = results.getStats().getWriteProgress().getRows().intValue();
                        }
                    } else {
                        // if data is empty, use writeProgress.rows
                        currentUpdateCount = results.getStats().getWriteProgress().getRows().intValue();
                    }
                } else {
                    currentUpdateCount = results.getStats().getWriteProgress().getRows().intValue();
                }
            }
            return true;
        } catch (RuntimeException e) {
            throw new SQLException(
                    "Error executing query: " + "SQL: " + sql + ", error = " + e.getMessage() + ", cause: " + e.getCause(), e);
        } finally {
            executingClient.set(null);
            if (currentResult.get() == null) {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (client != null) {
                    client.close();
                }
            }
        }
    }

    final boolean isQueryStatement(String sql) {
        return sql.toLowerCase().startsWith("select") || sql.toLowerCase().startsWith("show");
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException {
        checkOpen();
        return currentResult.get();
    }

    @Override
    public int getUpdateCount()
            throws SQLException {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults()
            throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public int getFetchDirection()
            throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int i)
            throws SQLException {

    }

    @Override
    public int getFetchSize()
            throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int i)
            throws SQLException {

    }

    @Override
    public int getResultSetConcurrency()
            throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType()
            throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String s)
            throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public void clearBatch()
            throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public int[] executeBatch()
            throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public Connection getConnection()
            throws SQLException {
        return connection();
    }

    @Override
    public boolean getMoreResults(int i)
            throws SQLException {
        checkOpen();
        if (i == CLOSE_CURRENT_RESULT) {
            closeResultSet();
            return false;
        }

        if (i != KEEP_CURRENT_RESULT) {
            throw new SQLException("Invalid value for getMoreResults: " + i);
        }
        throw new SQLFeatureNotSupportedException("Multiple results not supported");
    }

    @Override
    public ResultSet getGeneratedKeys()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getGeneratedKeys");
    }

    @Override
    public int executeUpdate(String s, int i)
            throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String s, int[] ints)
            throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String s, String[] strings)
            throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String s, int i)
            throws SQLException {
        return execute(s);
    }

    @Override
    public boolean execute(String s, int[] ints)
            throws SQLException {
        return execute(s);
    }

    @Override
    public boolean execute(String s, String[] strings)
            throws SQLException {
        return execute(s);
    }

    @Override
    @NotImplemented
    public int getResultSetHoldability() throws SQLException {
        // N/A applicable as we do not support transactions
        return 0;
    }

    @Override
    public boolean isClosed()
            throws SQLException {
        return connection.get() == null;
    }

    @Override
    public boolean isPoolable()
            throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void setPoolable(boolean b)
            throws SQLException {
        checkOpen();
    }

    @Override
    public void closeOnCompletion()
            throws SQLException {
        checkOpen();
        closeOnCompletion.set(true);
    }

    @Override
    public boolean isCloseOnCompletion()
            throws SQLException {
        checkOpen();
        return closeOnCompletion.get();
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass)
            throws SQLException {
        return aClass.isInstance(this);
    }

    protected final void checkOpen()
            throws SQLException {
        connection();
    }

    protected final DatabendConnection connection()
            throws SQLException {
        DatabendConnection connection = this.connection.get();
        if (connection == null) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
        return connection;
    }

    QueryLiveness queryLiveness() {
        DatabendResultSet r = currentResult.get();

        if (r != null) {
            return r.getLiveness();
        }
        return null;
    }
}
