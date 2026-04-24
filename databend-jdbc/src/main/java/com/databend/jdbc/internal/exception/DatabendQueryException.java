package com.databend.jdbc.internal.exception;

public class DatabendQueryException extends DatabendOperationException {
    public DatabendQueryException(String message) {
        super(message);
    }

    public DatabendQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
