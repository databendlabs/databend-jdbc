package com.databend.jdbc;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


public class URLUtils {
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
