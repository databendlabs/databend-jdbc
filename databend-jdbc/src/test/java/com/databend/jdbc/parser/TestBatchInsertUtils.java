package com.databend.jdbc.parser;

import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class TestBatchInsertUtils
{
    @Test(groups = "Unit")
    public void testFiles() {
        List<String[]> data = new ArrayList<>();
        data.add(new String[]{"1", "2", "{\"a\": 1, \"b\": \"2\"}", "hello, world 321"});
        BatchInsertUtils b = BatchInsertUtils.tryParseInsertSql("sq").get();
        File f = b.saveBatchToCSV(data);
        System.out.println(f.getAbsolutePath());
        try {
            FileReader fr = new FileReader(f);
            char[] buf = new char[1024];
            int len = fr.read(buf);
            System.out.println(new String(buf, 0, len));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
