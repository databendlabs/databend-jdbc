package com.databend.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Headers;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PresignContext {
    private final PresignMethod method;
    private final String stageName;
    private final String fileName;
    private final Headers headers;
    private final String url;

    private PresignContext(PresignMethod method, String stageName, String fileName, Headers headers, String url) {
        this.method = method;
        this.stageName = stageName;
        this.fileName = fileName;
        this.headers = headers;
        this.url = url;
    }

    public static void createStageIfNotExists(DatabendConnection connection, String stageName) throws SQLException {
        String sql = String.format("CREATE STAGE IF NOT EXISTS %s", stageName);
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    public static void dropStageIfExists(DatabendConnection connection, String stageName) throws SQLException {
        String sql = String.format("DROP STAGE IF EXISTS %s", stageName);
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    public static PresignContext getPresignContext(DatabendConnection connection, PresignMethod method, String stageName, String fileName)
            throws SQLException, JsonProcessingException {
        requireNonNull(connection, "connection is null");
        requireNonNull(method, "method is null");
        Statement statement = connection.createStatement();
        try {
            DatabendResultSet resultSet = (DatabendResultSet) statement.executeQuery(buildRequestSQL(method, stageName, fileName));
            if (resultSet.next()) {
                Headers headers = getHeaders(resultSet.getString("headers"));
                String url = resultSet.getString("url");
                return builder()
                        .method(method)
                        .stageName(stageName)
                        .fileName(fileName)
                        .headers(headers)
                        .url(url)
                        .build();
            } else {
                throw new SQLException("Failed to get presign url");
            }
        } catch (Throwable e) {
            try {
                statement.close();
            } catch (Throwable closeException) {
                if (closeException != e) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
    }

    public static String buildRequestSQL(PresignMethod method, String stageName, String fileName) {
        StringBuilder sql = new StringBuilder("PRESIGN ");
        sql.append(presignMethod(method));
        sql.append(" ");
        sql.append("@");
        if (stageName != null) {
            sql.append(stageName);
            sql.append("/");
        } else {
            sql.append("~/");
        }
        sql.append(fileName);
        return sql.toString();
    }

    private static String presignMethod(PresignMethod method) {
        switch (method) {
            case UPLOAD:
                return "UPLOAD";
            case DOWNLOAD:
            default:
                return "DOWNLOAD";
        }
    }

    private static Headers getHeaders(String headers)
            throws JsonProcessingException {
        Map<String, Object> resp = new ObjectMapper().readValue(headers, HashMap.class);
        Headers.Builder builder = new Headers.Builder();
        for (Map.Entry<String, Object> entry : resp.entrySet()) {
            builder.add(entry.getKey(), entry.getValue().toString());
        }
        return builder.build();
    }

    public static PresignContext.Builder builder() {
        return new PresignContext.Builder();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("method", method)
                .add("stageName", stageName)
                .add("fileName", fileName)
                .add("headers", headers)
                .add("url", url)
                .toString();
    }

    public PresignMethod getMethod() {
        return method;
    }

    public String getStageName() {
        return stageName;
    }

    public String getFileName() {
        return fileName;
    }

    public Headers getHeaders() {
        return headers;
    }

    public String getUrl() {
        return url;
    }

    enum PresignMethod {
        UPLOAD,
        DOWNLOAD
    }

    // builder pattern
    public static class Builder {
        private PresignMethod method;
        private String stageName;
        private String fileName;
        private long fileSize;
        private Headers headers;
        private String url;

        public Builder method(PresignMethod method) {
            this.method = method;
            return this;
        }

        public Builder stageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder fileSize(Long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public Builder headers(Headers headers) {
            this.headers = headers;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public PresignContext build() {
            return new PresignContext(method, stageName, fileName, headers, url);
        }
    }

}
