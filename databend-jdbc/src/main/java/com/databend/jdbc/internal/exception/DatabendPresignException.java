package com.databend.jdbc.internal.exception;

public class DatabendPresignException extends DatabendOperationException {
    public DatabendPresignException(String message) {
        super(message);
    }

    public DatabendPresignException(String message, Throwable cause) {
        super(message, cause);
    }
}
