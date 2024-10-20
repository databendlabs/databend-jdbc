package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendStage;
import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;
import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Files;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestFileTransfer {
    private static byte[] streamToByteArray(InputStream stream) throws IOException {

        byte[] buffer = new byte[1024];
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        int line = 0;
        // read bytes from stream, and store them in buffer
        while ((line = stream.read(buffer)) != -1) {
            // Writes bytes from byte array (buffer) into output stream.
            os.write(buffer, 0, line);
        }
        os.flush();
        os.close();
        return os.toByteArray();
    }

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create table
        Connection c = Utils.createConnection();

        c.createStatement().execute("drop table if exists copy_into");
        c.createStatement().execute("CREATE TABLE IF NOT EXISTS copy_into (i int, a Variant, b string) ENGINE = FUSE");
    }

    // generate a csv file in a temp directory with given lines, return absolute path of the generated csv
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return csvPath;
    }

    private String generateLargeCSV() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String csvPath = tmpDir + "/large_test.csv";
        long fileSizeInBytes = 0;
        File f = new File(csvPath);
        try {
            FileWriter writer = new FileWriter(f);
            while (fileSizeInBytes < 2L * 1024 * 1024 * 1024) { // 2GB
                for (int i = 0; i < 1000; i++) { // write 1000 lines at a time
                    int num = (int) (Math.random() * 1000);
                    writer.write("a,b,c," + num + "\n");
                }
                writer.flush();
                fileSizeInBytes = f.length();
            }
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return csvPath;
    }

    private String generateRandomCSVComplex(int lines) {
        if (lines <= 0) {
            return "";
        }
        String tmpDir = System.getProperty("java.io.tmpdir");
        String csvPath = tmpDir + "/complex.csv";
        try {
            FileWriter writer = new FileWriter(csvPath);
            CsvWriter w = CsvWriter.builder().quoteCharacter('"').lineDelimiter(LineDelimiter.LF).build(writer);
            for (int i = 0; i < lines; i++) {
                w.writeRow("1", "{\"str_col\": 1, \"int_col\": 2}", "c");
            }
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return csvPath;
    }

    @Test(groups = {"IT"})
    public void testFileTransfer()
            throws IOException {
        String filePath = generateRandomCSV(10000);
        File f = new File(filePath);
        InputStream downloaded = null;
        try (FileInputStream fileInputStream = new FileInputStream(f)) {
            Connection connection = Utils.createConnection();
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", f.length(), false);
            downloaded = databendConnection.downloadStream(stageName, "jdbc/test/test.csv", false);
            byte[] arr = streamToByteArray(downloaded);
            Assert.assertEquals(arr.length, f.length());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (downloaded != null) {
                downloaded.close();
            }
        }
    }

    @Test(groups = {"Local", "SKIP_NGINX"})
    public void testFileTransferThroughAPI() {
        String filePath = generateRandomCSV(100000);
        File f = new File(filePath);
        try (InputStream fileInputStream = Files.newInputStream(f.toPath())) {
            Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.ALL);

            Connection connection = Utils.createConnectionWithPresignedUrlDisable();
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", f.length(), false);
            InputStream downloaded = databendConnection.downloadStream(stageName, "jdbc/test/test.csv", false);
            byte[] arr = streamToByteArray(downloaded);
            Assert.assertEquals(arr.length, f.length());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            f.delete();
        }
    }

    @Test(groups = {"IT"})
    public void testCopyInto() {
        String filePath = generateRandomCSVComplex(10);
        File f = new File(filePath);
        try (FileInputStream fileInputStream = new FileInputStream(f)) {
            Connection connection = Utils.createConnection();
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/c2/", fileInputStream, "complex.csv", f.length(), false);
            fileInputStream.close();
            DatabendStage s = DatabendStage.builder().stageName(stageName).path("jdbc/c2/").build();
            DatabendCopyParams p = DatabendCopyParams.builder().setPattern("complex.csv").setDatabaseTableName("copy_into").setDatabendStage(s).build();
            databendConnection.copyIntoTable(null, "copy_into", p);
            Statement stmt = connection.createStatement();
            ResultSet r = stmt.executeQuery("SELECT * FROM copy_into");
            while (r.next()) {
                System.out.println(r.getInt(1) + " " + r.getString(2) + " " + r.getString(3));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
