package com.databend.jdbc.internal.exception;

public class DatabendStageUploadException extends DatabendOperationException {
    public DatabendStageUploadException(String message) {
        super(message);
    }

    public DatabendStageUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
