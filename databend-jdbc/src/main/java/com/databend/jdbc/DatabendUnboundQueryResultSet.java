package com.databend.jdbc;

import com.databend.jdbc.internal.query.QueryRowField;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * We create a new ResultSet class constructed directly from fixed values or pre-query combinations of data.
 * This class is created to do some specific processing on some implementations of the DatabaseMetaData interface.
 * <p>
 * Since some of the queries returned from databend will be some different from the jdbc standard,
 * so that we need to make some modifications to the types, values, etc. after the returned results.
 * The actual returned datas from the interface is a new ResultSet.
 */
class DatabendUnboundQueryResultSet extends AbstractDatabendResultSet {

    private boolean closed = false;

    DatabendUnboundQueryResultSet(Optional<Statement> statement, List<QueryRowField> schema, Iterator<List<Object>> results) {
        super(statement, schema, new IteratorResultCursor(results), null, "NotQueryResultSet");
    }

    @Override
    public void close() throws SQLException {
        try {
            Statement statement = getStatement();
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ignored) {
        }
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
