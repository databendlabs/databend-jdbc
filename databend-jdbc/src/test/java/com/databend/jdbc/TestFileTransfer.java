package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendStage;
import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TestFileTransfer
{
    @BeforeTest
    public void setUp()
            throws SQLException
    {
        // create table
        Connection c = createConnection();

        c.createStatement().execute("drop table if exists copy_into");
        c.createStatement().execute("CREATE TABLE IF NOT EXISTS copy_into (i int, a Variant, b string) ENGINE = FUSE");
    }
    private static byte[] streamToByteArray(InputStream stream) throws IOException
    {

        byte[] buffer = new byte[1024];
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        int line = 0;
        // read bytes from stream, and store them in buffer
        while ((line = stream.read(buffer)) != -1) {
            // Writes bytes from byte array (buffer) into output stream.
            os.write(buffer, 0, line);
        }
        stream.close();
        os.flush();
        os.close();
        return os.toByteArray();
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "root", "root");
    }

    private Connection createConnection(boolean presignDisabled) throws SQLException
    {
        String url = "jdbc:databend://localhost:8000?presigned_url_disabled=" + presignDisabled;
        return DriverManager.getConnection(url, "root", "root");
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
        }
        catch (Exception e) {
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
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return csvPath;
    }

    @Test(groups = {"IT"})
    public void testFileTransfer()
    {
        String filePath = generateRandomCSV(10000);
        try {
            Connection connection = createConnection();
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            File f = new File(filePath);
            FileInputStream fileInputStream = new FileInputStream(f);
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", false);
            InputStream inputStream = databendConnection.downloadStream(stageName, "jdbc/test/test.csv", false);
            Assert.assertNotNull(inputStream);
            byte[] got = streamToByteArray(inputStream);
            byte[] expected = streamToByteArray(new FileInputStream(f));
            Assert.assertEquals(got, expected);
            inputStream.close();
            fileInputStream.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        } finally{

        }
    }
    @Test(groups = {"Local"})
    public void testFileTransferThroughAPI()
    {
        String filePath = generateRandomCSV(100000);
        try {
            Connection connection = createConnection(true);
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            File f = new File(filePath);
            InputStream fileInputStream = Files.newInputStream(f.toPath());
            databendConnection.uploadStream(stageName, "jdbc/test/", fileInputStream, "test.csv", false);
            InputStream inputStream = databendConnection.downloadStream(stageName, "jdbc/test/test.csv", false);
            Assert.assertNotNull(inputStream);
            byte[] got = streamToByteArray(inputStream);
            byte[] expected = streamToByteArray(Files.newInputStream(f.toPath()));
            Assert.assertEquals(got, expected);
            inputStream.close();
            fileInputStream.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        } finally{
            File f = new File(filePath);
            f.delete();
        }
    }

    @Test(groups = {"IT"})
    public void testCopyInto()
    {
        String filePath = generateRandomCSVComplex(10);
        try {
            Connection connection = createConnection();
            String stageName = "test_stage";
            DatabendConnection databendConnection = connection.unwrap(DatabendConnection.class);
            PresignContext.createStageIfNotExists(databendConnection, stageName);
            File f = new File(filePath);
            FileInputStream fileInputStream = new FileInputStream(f);
            databendConnection.uploadStream(stageName, "jdbc/c2/", fileInputStream, "complex.csv", false);
            DatabendStage s = DatabendStage.builder().stageName(stageName).path("jdbc/c2/").build();
            DatabendCopyParams p = DatabendCopyParams.builder().setPattern("complex.csv").build();
            databendConnection.copyIntoTable(null, "copy_into", s, p);
            Statement stmt = connection.createStatement();
            ResultSet r = stmt.executeQuery("SELECT * FROM copy_into");
            while (r.next()) {
                System.out.println(r.getInt(1) + " " + r.getString(2) + " " + r.getString(3));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
