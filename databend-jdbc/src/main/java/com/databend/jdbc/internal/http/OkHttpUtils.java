package com.databend.jdbc.internal.http;

import com.google.common.base.CharMatcher;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Objects.requireNonNull;

public final class OkHttpUtils {
    private OkHttpUtils() {
    }

    public static Interceptor userAgentInterceptor(String userAgent) {
        return chain -> chain.proceed(chain.request().newBuilder().header("User-Agent", userAgent).build());
    }

    public static Interceptor basicAuthInterceptor(String username, String password) {
        return chain -> chain.proceed(chain.request().newBuilder().header("Authorization", Credentials.basic(username, password)).build());
    }

    public static Interceptor tokenAuth(String accessToken) {
        requireNonNull(accessToken, "accessToken is null");
        checkArgument(CharMatcher.inRange((char) 33, (char) 126).matchesAllOf(accessToken));
        return chain -> chain.proceed(chain.request().newBuilder()
                .addHeader(AUTHORIZATION, "Bearer " + accessToken)
                .build());
    }

    public static void setupTimeouts(OkHttpClient.Builder clientBuilder, int timeout, TimeUnit unit) {
        clientBuilder
                .connectTimeout(timeout, unit)
                .readTimeout(timeout, unit)
                .writeTimeout(timeout, unit);
    }

    public static void setupInsecureSsl(OkHttpClient.Builder clientBuilder) {
        // Reuse one TLS identity so secure Databend clients can reuse pooled connections. This is
        // applied conditionally by DatabendDriverUri; plain HTTP clients keep the default TLS setup.
        InsecureSslConfig config = InsecureSslConfig.get();
        clientBuilder.sslSocketFactory(config.socketFactory, config.trustManager);
        clientBuilder.hostnameVerifier(config.hostnameVerifier);
    }

    private static final class InsecureSslConfig {
        private static volatile InsecureSslConfig instance;

        private final SSLSocketFactory socketFactory;
        private final X509TrustManager trustManager;
        private final HostnameVerifier hostnameVerifier;

        private InsecureSslConfig(SSLSocketFactory socketFactory, X509TrustManager trustManager,
                HostnameVerifier hostnameVerifier) {
            this.socketFactory = socketFactory;
            this.trustManager = trustManager;
            this.hostnameVerifier = hostnameVerifier;
        }

        private static InsecureSslConfig get() {
            InsecureSslConfig config = instance;
            if (config == null) {
                synchronized (InsecureSslConfig.class) {
                    config = instance;
                    if (config == null) {
                        config = create();
                        instance = config;
                    }
                }
            }
            return config;
        }

        private static InsecureSslConfig create() {
            X509TrustManager trustManager = createInsecureTrustManager();
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[] {trustManager}, new SecureRandom());
                return new InsecureSslConfig(
                        sslContext.getSocketFactory(),
                        trustManager,
                        (hostname, session) -> true);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException("Error setting up SSL: " + e.getMessage(), e);
            }
        }
    }

    private static X509TrustManager createInsecureTrustManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                throw new UnsupportedOperationException("checkClientTrusted should not be called");
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }
}
