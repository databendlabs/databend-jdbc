package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendPresignClient;
import com.databend.jdbc.cloud.DatabendPresignClientV1;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.io.*;
import java.sql.SQLException;

public class TestDatabendPresignClient {

    private String generateRandomCSV(int lines) throws IOException {
        if (lines <= 0) {
            return "";
        }
        String tmpDir = System.getProperty("java.io.tmpdir");
        String csvPath = tmpDir + "/test.csv";
        try (FileWriter writer = new FileWriter(csvPath)) {
            for (int i = 0; i < lines; i++) {
                int num = (int) (Math.random() * 1000);
                writer.write("a,b,c," + num + "\n");
            }
        }
        return csvPath;
    }

    @Test(groups = {"LOCAL"})
    public void uploadFileAPI() throws IOException, SQLException {
        String filePath = null;
        try (DatabendConnectionImpl connection = Utils.createConnection().unwrap(DatabendConnectionImpl.class)) {
            OkHttpClient client = connection.getHttpClient();
            DatabendPresignClient presignClient = new DatabendPresignClientV1(client, connection.getURI().toString());
            filePath = generateRandomCSV(10);
            File file = new File(filePath);
            InputStream inputStream = new FileInputStream(file);
            presignClient.presignUpload(null, inputStream, "~", "api/upload/", "1.csv", file.length(), true);
        } finally {
            //remove temp file
            if (filePath != null) {
                File file = new File(filePath);
                file.delete();
            }
        }
    }
}
