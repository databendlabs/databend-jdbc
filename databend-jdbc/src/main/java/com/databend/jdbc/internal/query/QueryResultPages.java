package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.session.SessionState;
import okhttp3.Request;

import java.io.Closeable;
import java.util.List;

public interface QueryResultPages extends Closeable {
    String getQuery();

    @Override
    void close();

    SessionState getSession();

    String getNodeID();

    QueryResults getResults();

    List<QueryRowField> getSchema();

    ResultPage getPage();

    boolean execute(Request request);

    boolean advance();

    boolean hasNext();
}
