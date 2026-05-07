package com.databend.jdbc.internal.http;

import java.io.IOException;

public final class RetryableHttpStatusException extends IOException {
    public RetryableHttpStatusException(String message) {
        super(message);
    }
}
