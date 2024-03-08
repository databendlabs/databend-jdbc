package com.databend.jdbc.exception;


public class DatabendFailedToPingException extends RuntimeException {
    public DatabendFailedToPingException() {
        super();
    }

    public DatabendFailedToPingException(String message) {
        super(message);
    }

    public DatabendFailedToPingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabendFailedToPingException(Throwable cause) {
        super(cause);
    }

    protected DatabendFailedToPingException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
