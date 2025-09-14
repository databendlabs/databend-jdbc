package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendStage;
import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;
import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Files;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestFileTransfer {
    private static byte[] streamToByteArray(InputStream stream) throws IOException {

        byte[] buffer = new byte[1024];
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        int line;
        // read bytes from stream, and store them in buffer
        while ((line = stream.read(buffer)) != -1) {
            // Writes bytes from byte array (buffer) into output stream.
            os.write(buffer, 0, line);
        }
        os.flush();
        os.close();
        return os.toByteArray();
    }

    @BeforeTest(groups = "IT")
    public void setUp()
            throws SQLException {
        // create table
        try (Connection c = Utils.createConnection()) {

            c.createStatement().execute("drop table if exists copy_into");
            c.createStatement().execute("CREATE TABLE IF NOT EXISTS copy_into (i int, a Variant, b string) ENGINE = FUSE");
        }
    }

    // generate a csv file in a temp directory with given lines, return absolute path of the generated csv
    private String generateRandomCSV(int lines) throws IOException {
        if (lines <= 0) {
            return "";
        }
        String tmpDir = System.getProperty("java.io.tmpdir");
        String csvPath = tmpDir + "/" + UUID.randomUUID();
        try (FileWriter writer = new FileWriter(csvPath)) {
            for (int i = 0; i < lines; i++) {
                int num = (int) (Math.random() * 1000);
                writer.write("a,b,c," + num + "\n");
            }
        }
        return csvPath;
    }

//    private String generateLargeCSV() {
//        String tmpDir = System.getProperty("java.io.tmpdir");
//        String csvPath = tmpDir + "/large_test.csv";
//        long fileSizeInBytes = 0;
//        File f = new File(csvPath);
//        try {
//            FileWriter writer = new FileWriter(f);
//            while (fileSizeInBytes < 2L * 1024 * 1024 * 1024) { // 2GB
//                for (int i = 0; i < 1000; i++) { // write 1000 lines at a time
//                    int num = (int) (Math.random() * 1000);
//                    writer.write("a,b,c," + num + "\n");
//                }
//                writer.flush();
//                fileSizeInBytes = f.length();
//            }
//            writer.close();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return csvPath;
//    }

    private String generateRandomCSVComplex(int lines) throws IOException {
        if (lines <= 0) {
            return "";
        }
        String tmpDir = System.getProperty("java.io.tmpdir");
        String csvPath = tmpDir + "/complex.csv";
        try (FileWriter writer = new FileWriter(csvPath)) {
            CsvWriter w = CsvWriter.builder().quoteCharacter('"').lineDelimiter(LineDelimiter.LF).build(writer);
            for (int i = 0; i < lines; i++) {
                w.writeRow(String.valueOf(i), "{\"str_col\": 1, \"int_col\": 2}", "c");
            }
        }
        return csvPath;
    }

    @Test(groups = {"IT"})
    public void testFileTransfer()
            throws IOException, SQLException {
        String filePath = generateRandomCSV(10000);
        File f = new File(filePath);
        InputStream downloaded = null;
        try (FileInputStream fileInputStream = new FileInputStream(f);
             Connection connection = Utils.createConnection()) {
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(connection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", f.length(), false);
            downloaded = databendConnection.downloadStream(stageName, "jdbc/test/test.csv");
            byte[] arr = streamToByteArray(downloaded);
            Assert.assertEquals(arr.length, f.length());
        } finally {
            if (downloaded != null) {
                downloaded.close();
            }
        }
    }

    @Test(groups = {"IT"})
    public void testFileTransferThroughAPI() throws SQLException, IOException {
        String filePath = generateRandomCSV(10000);
        File f = new File(filePath);
        try (InputStream fileInputStream = Files.newInputStream(f.toPath());
             Connection connection = Utils.createConnectionWithPresignedUrlDisable()) {
            Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.ALL);


            String stageName = "test_stage_np";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(connection, stageName);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", f.length(), false);
            InputStream downloaded = databendConnection.downloadStream(stageName, "jdbc/test/test.csv");
            byte[] arr = streamToByteArray(downloaded);
            Assert.assertEquals(arr.length, f.length());
        } finally {
            f.delete();
        }
    }

    @Test(groups = {"IT"})
    public void testCopyInto() throws IOException, SQLException {
        String filePath = generateRandomCSVComplex(10);
        File f = new File(filePath);
        try (FileInputStream fileInputStream = new FileInputStream(f)) {
            Connection connection = Utils.createConnection();
            String stageName = "test_stage";
            FileTransferAPI databendConnection = connection.unwrap(FileTransferAPI.class);
            PresignContext.createStageIfNotExists(connection, stageName);
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
        }
    }

    @DataProvider(name = "streamingLoad")
    private Object[][] provideTestData() {
        return new Object[][] {
                {"STREAMING"},
                {"STAGE"}
        };
    }

    @Test(groups = "IT", dataProvider = "streamingLoad")
    public void testLoadStreamToTable(String method) throws IOException, SQLException {
        if (!Compatibility.driverCapability.streamingLoad) {
            System.out.println("Skip testLoadStreamToTableInner: driver version too low");
            return;
        }
        if (!Compatibility.serverCapability.streamingLoad) {
            System.out.println("Skip testLoadStreamToTableInner: server version too low");
            return;
        }
        System.out.println("testLoadStreamToTableInner " + method);
        String filePath = generateRandomCSVComplex(10);
        File f = new File(filePath);
        try (FileInputStream fileInputStream = new FileInputStream(f);
             Connection connection = Utils.createConnectionWithPresignedUrlDisable();
             Statement statement = connection.createStatement()) {
            String dbName = "test_load_stream_" + method;
            statement.execute(String.format("create or replace database %s", dbName));
            statement.execute(String.format("use %s", dbName));
            statement.execute("create or replace table test_load(i int, a Variant, b string)");
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            String sql = "insert into test_load from @_databend_load file_format=(type=csv)";
            int nUpdate = databendConnection.loadStreamToTable(sql, fileInputStream, f.length(), DatabendConnection.LoadMethod.valueOf(method));
            Assert.assertEquals(nUpdate, 10);
            fileInputStream.close();
            ResultSet r = statement.executeQuery("SELECT * FROM test_load");
            int n = 0;
            while (r.next()) {
                Assert.assertEquals(r.getInt(1), n);
                n += 1;
            }
            Assert.assertEquals(10, n);
        }
    }
}
