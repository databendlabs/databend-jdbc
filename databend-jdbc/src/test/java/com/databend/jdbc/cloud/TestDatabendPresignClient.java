package com.databend.jdbc.cloud;

import com.databend.jdbc.DatabendConnection;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestDatabendPresignClient
{
    private Connection createConnection()
            throws SQLException
    {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "root", "root");
    }
    private String generateRandomCSV(int lines) {
        if (lines <= 0) {
            return "";
        }
        String tmpDir = System.getProperty("java.io.tmpdir");
        String csvPath = tmpDir + "/test.csv";
        try {
            FileWriter writer = new FileWriter(csvPath);
            for (int i = 0; i < lines; i++) {
                int num = (int) (Math.random() * 1000);
                writer.write("a,b,c," + num + "\n");
            }
            writer.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return csvPath;
    }
    @Test(groups = {"Local"})
    public void uploadFileAPI() {
        String filePath = null;
        try (DatabendConnection connection = createConnection().unwrap(DatabendConnection.class)) {
            OkHttpClient client = connection.getHttpClient();
            DatabendPresignClient presignClient = new DatabendPresignClientV1(client, connection.getURI().toString());
            filePath = generateRandomCSV(10);
            File file = new File(filePath);
            InputStream inputStream = new FileInputStream(file);
            presignClient.presignUpload(null, inputStream, "~", "api/upload/", "1.csv", true);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //remove temp file
            if (filePath != null) {
                File file = new File(filePath);
                file.delete();
            }
        }
    }
}
