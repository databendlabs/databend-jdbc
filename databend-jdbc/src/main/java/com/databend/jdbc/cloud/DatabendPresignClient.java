package com.databend.jdbc.cloud;

import okhttp3.Headers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface DatabendPresignClient
{
    public void presignUpload(File srcFile, InputStream inputStream, Headers headers, String presignedUrl, boolean uploadFromStream) throws IOException;
    public void presignDownload(String destFileName, Headers headers, String presignedUrl);
    public InputStream presignDownloadStream(Headers headers, String presignedUrl);
}
