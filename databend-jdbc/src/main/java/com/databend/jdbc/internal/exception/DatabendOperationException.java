package com.databend.jdbc.internal.exception;

public class DatabendOperationException extends RuntimeException {
    public DatabendOperationException(String message) {
        super(message);
    }

    public DatabendOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
