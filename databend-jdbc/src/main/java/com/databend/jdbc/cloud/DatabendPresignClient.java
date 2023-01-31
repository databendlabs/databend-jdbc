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

    /**
     * presignUpload file through databend api instead of presigned url, it should only be adopted if presigned url is not available
     * @param srcFile the file to be uploaded
     * @param inputStream the input stream to be uploaded
     * @param uploadFromStream whether the upload is from stream
     * @throws IOException
     */
    public void presignUpload(File srcFile, InputStream inputStream, String stageName, String relativePath,  boolean uploadFromStream) throws IOException;
}
