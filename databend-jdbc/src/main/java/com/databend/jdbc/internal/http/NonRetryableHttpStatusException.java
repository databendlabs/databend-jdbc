package com.databend.jdbc.internal.http;

import java.io.IOException;

public final class NonRetryableHttpStatusException extends IOException {
    public NonRetryableHttpStatusException(String message) {
        super(message);
    }
}
