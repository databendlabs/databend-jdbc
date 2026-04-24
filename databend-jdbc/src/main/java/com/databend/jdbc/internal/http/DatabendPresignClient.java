package com.databend.jdbc.internal.http;

import okhttp3.Headers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface DatabendPresignClient {
    void presignUpload(File srcFile, InputStream inputStream, Headers headers, String presignedUrl, long fileSize, boolean uploadFromStream) throws IOException;

    void presignDownload(String destFileName, Headers headers, String presignedUrl);

    InputStream presignDownloadStream(Headers headers, String presignedUrl);
}
