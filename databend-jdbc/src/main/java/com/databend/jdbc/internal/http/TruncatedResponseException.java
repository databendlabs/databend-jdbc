package com.databend.jdbc.internal.http;

import java.io.IOException;

/**
 * Raised when a response body ends before its own framing says it should.
 *
 * <p>An otherwise normally completed HTTP response can contain a truncated
 * application payload. Detection then happens while decoding. This type carries
 * that outcome back into the retry policy without depending on exception wording.
 * HTTP framing still detects incomplete fixed-length or chunked transfers.
 */
public final class TruncatedResponseException extends IOException {
    public TruncatedResponseException(String message) {
        super(message);
    }
}
