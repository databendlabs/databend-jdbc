package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.session.SessionState;
import okhttp3.Request;

import java.io.Closeable;
import java.util.Map;

public interface QueryResultPages extends Closeable {
    String getQuery();

    @Override
    void close();

    SessionState getSession();

    String getNodeID();

    Map<String, String> getAdditionalHeaders();

    QueryResults getResults();

    boolean execute(Request request);

    boolean advance();

    boolean hasNext();
}
