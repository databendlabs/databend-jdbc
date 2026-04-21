package com.databend.jdbc.internal.exception;

public class DatabendSessionException extends DatabendOperationException {
    public DatabendSessionException(String message) {
        super(message);
    }

    public DatabendSessionException(String message, Throwable cause) {
        super(message, cause);
    }
}
