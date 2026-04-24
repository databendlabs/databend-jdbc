package com.databend.jdbc;

import com.databend.jdbc.internal.data.DatabendRawType;
import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.QueryResults;
import com.databend.jdbc.internal.query.QueryRowField;
import com.databend.jdbc.internal.query.ResultPage;
import com.databend.jdbc.internal.session.Capability;
import com.databend.jdbc.internal.session.SessionState;
import com.vdurmont.semver4j.Semver;
import okhttp3.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestDatabendStatement {
    @Test(groups = {"UNIT"})
    public void testShouldUseSyntheticResultSetWhenNoSchemaAndNoData() throws SQLException {
        FakeQueryResultPages queryPages = new FakeQueryResultPages(
                Collections.<QueryRowField>emptyList(),
                emptyResults(Collections.<QueryRowField>emptyList(), Collections.<String, String>emptyMap()));

        Assert.assertTrue(DatabendStatement.shouldUseSyntheticResultSet("create table t(a int)", queryPages, 0));
    }

    @Test(groups = {"UNIT"})
    public void testShouldNotUseSyntheticResultSetWhenSchemaPresent() throws SQLException {
        List<QueryRowField> schema = Collections.singletonList(new QueryRowField("a", new DatabendRawType("Int32")));
        FakeQueryResultPages queryPages = new FakeQueryResultPages(
                Collections.<QueryRowField>emptyList(),
                emptyResults(schema, Collections.<String, String>emptyMap()));

        Assert.assertFalse(DatabendStatement.shouldUseSyntheticResultSet("create table t(a int)", queryPages, 0));
    }

    @Test(groups = {"UNIT"})
    public void testRealEmptyResultSetRetainsMetadataWhenSchemaPresent() throws SQLException {
        List<QueryRowField> schema = Collections.singletonList(new QueryRowField("a", new DatabendRawType("Int32")));
        FakeQueryResultPages queryPages = new FakeQueryResultPages(
                schema,
                emptyResults(schema, Collections.<String, String>emptyMap()));

        DatabendResultSet resultSet = DatabendResultSet.create(newStatementStub(), queryPages, 0, new Capability(new Semver("1.2.900")));
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            Assert.assertEquals(metaData.getColumnCount(), 1);
            Assert.assertEquals(metaData.getColumnLabel(1), "a");
            Assert.assertFalse(resultSet.next());
        } finally {
            resultSet.close();
        }
    }

    private static QueryResults emptyResults(List<QueryRowField> schema, Map<String, String> settings) {
        return new QueryResults(
                "qid",
                "node",
                null,
                SessionState.createDefault(),
                schema,
                Collections.<List<String>>emptyList(),
                settings,
                "Succeeded",
                null,
                null,
                null,
                30,
                null,
                null,
                URI.create("/v1/query/final"),
                null);
    }

    private static Statement newStatementStub() {
        return (Statement) Proxy.newProxyInstance(
                TestDatabendStatement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, args) -> {
                    Class<?> returnType = method.getReturnType();
                    if (returnType == boolean.class) {
                        return false;
                    }
                    if (returnType == int.class) {
                        return 0;
                    }
                    if (returnType == long.class) {
                        return 0L;
                    }
                    if (returnType == float.class) {
                        return 0f;
                    }
                    if (returnType == double.class) {
                        return 0d;
                    }
                    return null;
                });
    }

    private static final class FakeQueryResultPages implements QueryResultPages {
        private final List<QueryRowField> schema;
        private final QueryResults results;

        private FakeQueryResultPages(List<QueryRowField> schema, QueryResults results) {
            this.schema = schema;
            this.results = results;
        }

        @Override
        public String getQuery() {
            return "select 1";
        }

        @Override
        public void close() {
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
            return results;
        }

        @Override
        public List<QueryRowField> getSchema() {
            return schema;
        }

        @Override
        public ResultPage getPage() {
            return null;
        }

        @Override
        public boolean execute(Request request) {
            return false;
        }

        @Override
        public boolean advance() {
            return false;
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    }
}
