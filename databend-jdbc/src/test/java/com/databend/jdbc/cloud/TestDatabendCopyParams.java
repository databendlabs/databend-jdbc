package com.databend.jdbc.cloud;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestDatabendCopyParams {
    @Test(groups = {"Unit"})
    public void testDatabendStage() {
        DatabendCopyParams prms = DatabendCopyParams.builder()
                .setPattern("*.csv")
                .setType("XML")
                .build();
        assertEquals(prms.getPattern(), "*.csv");
        assertEquals(prms.getType(), "XML");
        assertEquals(prms.toString().trim(), "PATTERN = '*.csv' FILE_FORMAT = ( type = 'XML' )");
        prms = DatabendCopyParams.builder().build();
        assertEquals(prms.getPattern(), null);
        assertEquals(prms.getType(), "CSV");
        assertEquals(prms.toString().trim(), "FILE_FORMAT = ( type = 'CSV' )");
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        Map<String, String> fileOptions = new HashMap<>();
        fileOptions.put("RECORD_DELIMITER", "0x0A");
        fileOptions.put("FIELD_DELIMITER", "0x09");
        fileOptions.put("SKIP_HEADER", "1");
        fileOptions.put("ESCAPE", "0x5C");
        fileOptions.put("QUOTE", "0x22");
        fileOptions.put("ROW_TAG", "test");
        fileOptions.put("NULL_DISPLAY", "NULL");
        Map<String, String> copyOptions = new HashMap<>();
        copyOptions.put("ON_ERROR", "continue");
        copyOptions.put("PURGE", "true");
        copyOptions.put("FORCE", "true");
        copyOptions.put("SIZE_LIMIT", "1000");
        DatabendStage s = DatabendStage.builder().stageName("~").path("jdbc/c2/").build();
        prms = DatabendCopyParams.builder().setFiles(files).setDatabendStage(s).setPattern("a.txt").setType("parquet")
                .setCopyOptions(copyOptions).setFileOptions(fileOptions).build();
        assertEquals(prms.getDatabendStage().getStageName(),"~");
        assertEquals(prms.getDatabendStage().getPath(),"jdbc/c2/");
        assertEquals(prms.getPattern(), "a.txt");
        assertEquals(prms.getType(), "parquet");
        assertEquals(prms.getFiles().size(), 2);
        assertEquals(prms.getFiles().get(0), "file1");
        assertEquals(prms.getFiles().get(1), "file2");
        assertEquals(prms.getFileOptions().size(), 7);
        assertEquals(prms.getFileOptions().get("RECORD_DELIMITER"), "0x0A");
        assertEquals(prms.getFileOptions().get("FIELD_DELIMITER"), "0x09");
        assertEquals(prms.getFileOptions().get("SKIP_HEADER"), "1");
        assertEquals(prms.getFileOptions().get("ESCAPE"), "0x5C");
        assertEquals(prms.getFileOptions().get("QUOTE"), "0x22");
        assertEquals(prms.getFileOptions().get("ROW_TAG"), "test");
        assertEquals(prms.getFileOptions().get("NULL_DISPLAY"), "NULL");
        assertEquals(prms.getCopyOptions().size(), 4);
        assertEquals(prms.getCopyOptions().get("ON_ERROR"), "continue");
        assertEquals(prms.getCopyOptions().get("PURGE"), "true");
        assertEquals(prms.getCopyOptions().get("FORCE"), "true");
        assertEquals(prms.getCopyOptions().get("SIZE_LIMIT"), "1000");
        assertEquals(prms.toString().trim(), "FILES = ('file1','file2') PATTERN = 'a.txt' FILE_FORMAT = ( type = 'parquet' RECORD_DELIMITER = '0x0A' QUOTE = '0x22' ROW_TAG = 'test' NULL_DISPLAY = 'NULL' FIELD_DELIMITER = '0x09' SKIP_HEADER = 1 ESCAPE = '0x5C' ) PURGE = true FORCE = true SIZE_LIMIT = 1000 ON_ERROR = continue");
    }
}
