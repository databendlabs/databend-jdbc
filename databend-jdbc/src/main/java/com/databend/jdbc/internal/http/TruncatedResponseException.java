package com.databend.jdbc.internal.http;

import java.io.IOException;

/**
 * Raised when a response body ends before its own framing says it should.
 *
 * <p>A truncated body is indistinguishable from a complete one at the transport
 * layer when the response is chunked, so detection happens while decoding. This
 * type carries that outcome back into the retry policy without depending on the
 * wording of an exception message.
 */
public final class TruncatedResponseException extends IOException {
    public TruncatedResponseException(String message) {
        super(message);
    }
}
