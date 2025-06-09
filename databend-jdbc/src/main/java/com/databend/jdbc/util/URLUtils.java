package com.databend.jdbc.util;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * URL utility class providing URL encoding functionality.
 */
public class URLUtils {
    /**
     * Encodes a string using URL encoding.
     *
     * @param target The string to be encoded.
     * @return The encoded string, or null if encoding fails.
     */
    @Nullable
    public static String urlEncode(String target) {
        String encodedTarget;
        try {
            encodedTarget = URLEncoder.encode(target, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException uex) {
            return null;
        }
        return encodedTarget;
    }
}
