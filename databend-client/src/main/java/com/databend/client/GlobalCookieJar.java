package com.databend.client;

import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalCookieJar implements CookieJar {
    private final Map<String, Cookie> cookieStore = new ConcurrentHashMap<>();

    @Override
    public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
        for (Cookie cookie : cookies) {
            cookieStore.put(cookie.name(), cookie);
        }
    }

    @Override
    public List<Cookie> loadForRequest(HttpUrl url) {
        return new ArrayList<>(cookieStore.values());
    }

    public void add(Cookie cookie) {
        cookieStore.put(cookie.name(), cookie);
    }
}
