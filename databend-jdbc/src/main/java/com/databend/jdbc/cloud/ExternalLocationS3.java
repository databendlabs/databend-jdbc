package com.databend.jdbc.cloud;

// https://databend.rs/doc/sql-commands/dml/dml-copy-into-table#externallocation
public class ExternalLocationS3 {
    // for example: s3://bucket_name/path
    private final String location;
    private final String endpointUrl;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final String region;
    private final boolean enableVirtualHostStyle;

    private ExternalLocationS3(String location, String endpointUrl, String accessKeyId, String secretAccessKey, String sessionToken, String region, boolean enableVirtualHostStyle) {
        this.location = location;
        this.endpointUrl = endpointUrl;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.region = region;
        this.enableVirtualHostStyle = enableVirtualHostStyle;
    }

    public static ExternalLocationS3.Builder builder() {
        return new ExternalLocationS3.Builder();
    }

    public String getLocation() {
        return location;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public String getRegion() {
        return region;
    }

    public boolean isEnableVirtualHostStyle() {
        return enableVirtualHostStyle;
    }

    //'s3://mybucket/data.csv.gz'
    //  CONNECTION = (
    //        ENDPOINT_URL = 'https://<endpoint-URL>'
    //        ACCESS_KEY_ID = '<your-access-key-ID>'
    //        SECRET_ACCESS_KEY = '<your-secret-access-key>')
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String loc = "'" + location + "'";
        sb.append(loc);
        sb.append(" CONNECTION = (");

        if (endpointUrl != null) {
            sb.append("ENDPOINT_URL = '");
            sb.append(endpointUrl);
            sb.append("'");
            sb.append(" ");
        }
        if (accessKeyId != null) {
            sb.append("ACCESS_KEY_ID = '");
            sb.append(accessKeyId);
            sb.append("'");
            sb.append(" ");
        }
        if (secretAccessKey != null) {
            sb.append("SECRET_ACCESS_KEY = '");
            sb.append(secretAccessKey);
            sb.append("'");
            sb.append(" ");
        }
        if (sessionToken != null) {
            sb.append("SESSION_TOKEN = '");
            sb.append(sessionToken);
            sb.append("'");
            sb.append(" ");
        }
        if (region != null) {
            sb.append("REGION = '");
            sb.append(region);
            sb.append("'");
            sb.append(" ");
        }
        sb.append("ENABLE_VIRTUAL_HOST_STYLE = '");
        sb.append(enableVirtualHostStyle);
        sb.append("'");
        sb.append(" ");
        sb.append(")");
        return sb.toString();
    }

    // builder pattern
    public static class Builder {
        private String location;
        private String endpointUrl;
        private String accessKeyId;
        private String secretAccessKey;
        private String sessionToken;
        private String region;
        private boolean enableVirtualHostStyle;

        public Builder setLocation(String location) {
            this.location = location;
            return this;
        }

        public Builder setEndpointUrl(String endpointUrl) {
            this.endpointUrl = endpointUrl;
            return this;
        }

        public Builder setAccessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        public Builder setSecretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        public Builder setSessionToken(String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setEnableVirtualHostStyle(boolean enableVirtualHostStyle) {
            this.enableVirtualHostStyle = enableVirtualHostStyle;
            return this;
        }

        public ExternalLocationS3 build() {
            return new ExternalLocationS3(location, endpointUrl, accessKeyId, secretAccessKey, sessionToken, region, enableVirtualHostStyle);
        }
    }
}
