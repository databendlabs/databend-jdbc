package com.databend.jdbc.internal.http;

import java.io.IOException;

public final class PresignRequestFailedException extends IOException {
    public PresignRequestFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
