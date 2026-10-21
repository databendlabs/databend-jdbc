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

    /**
     * Transfers the current page to the caller. Resource-owning implementations must
     * detach it so advancing or closing the query cannot close a consumer's page.
     */
    ResultPage takePage();

    boolean execute(Request request);

    /** Returns false at EOF or after close; throws on request/decode failure. */
    boolean advance();

    boolean hasNext();
}
