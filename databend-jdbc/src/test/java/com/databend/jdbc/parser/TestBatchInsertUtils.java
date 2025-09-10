package com.databend.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestBatchInsertUtils {
    @Test(groups = "UNIT")
    public void testFiles() throws IOException {
        List<String[]> data = new ArrayList<>();
        data.add(new String[]{"1", "2", "{\"a\": 1, \"b\": \"2\"}", "hello, world 321"});
        BatchInsertUtils b = new BatchInsertUtils("sq");
        File f = b.saveBatchToCSV(data);
        System.out.println(f.getAbsolutePath());
        try (FileReader fr = new FileReader(f)) {
            char[] buf = new char[1024];
            int len = fr.read(buf);
            String actual = new String(buf, 0, len);
            String exp = "1,2,\"{\"\"a\"\": 1, \"\"b\"\": \"\"2\"\"}\",\"hello, world 321\"\n";
            Assert.assertEquals(exp, actual);
        }
    }

    @Test(groups = "UNIT")
    public void testGetDatabaseTableName() {
        BatchInsertUtils b = new BatchInsertUtils("INSERT INTO tb01(id,d,x,x,x,x,xt,col1) VALUES");
        Assert.assertEquals("tb01", b.getDatabaseTableName());
        BatchInsertUtils b1 = new BatchInsertUtils("INSERT INTO db.tb_test VALUES");
        Assert.assertEquals("db.tb_test", b1.getDatabaseTableName());
        BatchInsertUtils b2 = new BatchInsertUtils("INSERT INTO tb01  (id,d,x,x,x,x,xt,col1) VALUES");
        Assert.assertEquals("tb01", b2.getDatabaseTableName());
        BatchInsertUtils b3 = new BatchInsertUtils("insert into tb01 values");
        Assert.assertEquals("tb01", b3.getDatabaseTableName());
        BatchInsertUtils b4 = new BatchInsertUtils("INSERT INTO `test`(`x`, `y`) VALUES (?, ?)");
        Assert.assertEquals("test", b4.getDatabaseTableName());
    }
}
