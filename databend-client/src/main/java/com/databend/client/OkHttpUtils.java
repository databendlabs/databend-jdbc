/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databend.client;

import com.google.common.base.CharMatcher;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import javax.net.ssl.SSLContext;
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
        try {
            X509TrustManager trustAllCerts = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    throw new UnsupportedOperationException("checkClientTrusted should not be called");
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    // skip validation of server certificate
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            };

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[]{trustAllCerts}, new SecureRandom());

            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustAllCerts);
            clientBuilder.hostnameVerifier((hostname, session) -> true);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Error setting up SSL: " + e.getMessage(), e);
        }
    }
}
