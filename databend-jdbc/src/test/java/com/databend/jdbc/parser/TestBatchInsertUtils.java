package com.databend.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class TestBatchInsertUtils {
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

    @Test(groups = "Unit")
    public void testGetDatabaseTableName() {
        BatchInsertUtils b = BatchInsertUtils.tryParseInsertSql("INSERT INTO tb01(id,d,x,x,x,x,xt,col1) VALUES").get();
        Assert.assertEquals("tb01", b.getDatabaseTableName());
        BatchInsertUtils b1 = BatchInsertUtils.tryParseInsertSql("INSERT INTO db.tb_test VALUES").get();
        Assert.assertEquals("db.tb_test", b1.getDatabaseTableName());
        BatchInsertUtils b2 = BatchInsertUtils.tryParseInsertSql("INSERT INTO tb01  (id,d,x,x,x,x,xt,col1) VALUES").get();
        Assert.assertEquals("tb01", b2.getDatabaseTableName());
        BatchInsertUtils b3 = BatchInsertUtils.tryParseInsertSql("insert into tb01 values").get();
        Assert.assertEquals("tb01", b3.getDatabaseTableName());
        BatchInsertUtils b4 = BatchInsertUtils.tryParseInsertSql("INSERT INTO `test`(`x`, `y`) VALUES (?, ?)").get();
        Assert.assertEquals("test", b4.getDatabaseTableName());

    }
}
