package com.databend.jdbc.internal.exception;

public class DatabendStreamingLoadException extends DatabendOperationException {
    public DatabendStreamingLoadException(String message) {
        super(message);
    }

    public DatabendStreamingLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
