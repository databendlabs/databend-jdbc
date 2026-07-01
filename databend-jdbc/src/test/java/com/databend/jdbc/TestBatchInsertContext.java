package com.databend.jdbc;

import com.databend.jdbc.internal.binding.BatchInsertContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestBatchInsertContext {
    @Test(groups = "UNIT")
    public void testFiles() throws IOException {
        List<String[]> data = new ArrayList<>();
        data.add(new String[]{"1", "2", "{\"a\": 1, \"b\": \"2\"}", "hello, world 321"});
        BatchInsertContext context = new BatchInsertContext("sq");
        File f = context.saveBatchToCSV(data);
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
        BatchInsertContext plainInsert = new BatchInsertContext("INSERT INTO tb01(id,d,x,x,x,x,xt,col1) VALUES");
        Assert.assertEquals("tb01", plainInsert.getDatabaseTableName());
        BatchInsertContext qualifiedInsert = new BatchInsertContext("INSERT INTO db.tb_test VALUES");
        Assert.assertEquals("db.tb_test", qualifiedInsert.getDatabaseTableName());
        BatchInsertContext insertWithColumns = new BatchInsertContext("INSERT INTO tb01  (id,d,x,x,x,x,xt,col1) VALUES");
        Assert.assertEquals("tb01", insertWithColumns.getDatabaseTableName());
        BatchInsertContext lowerCaseInsert = new BatchInsertContext("insert into tb01 values");
        Assert.assertEquals("tb01", lowerCaseInsert.getDatabaseTableName());
        BatchInsertContext quotedInsert = new BatchInsertContext("INSERT INTO `test`(`x`, `y`) VALUES (?, ?)");
        Assert.assertEquals("test", quotedInsert.getDatabaseTableName());
        BatchInsertContext settingsInsert = new BatchInsertContext(
                "settings (timezone='Asia/Shanghai') insert into db.tb_test values (?)");
        Assert.assertEquals("db.tb_test", settingsInsert.getDatabaseTableName());
        BatchInsertContext overwriteInsert = new BatchInsertContext("insert overwrite table db.tb_test values (?)");
        Assert.assertEquals("db.tb_test", overwriteInsert.getDatabaseTableName());
        BatchInsertContext replaceInsert = new BatchInsertContext("replace into `db`.`tb-test` on(id) values (?)");
        Assert.assertEquals("db.tb-test", replaceInsert.getDatabaseTableName());
        BatchInsertContext select = new BatchInsertContext("select 'insert into t values'");
        Assert.assertNull(select.getDatabaseTableName());
    }

    @Test(groups = "UNIT")
    public void testIsBatchInsert() {
        BatchInsertContext insertValues = new BatchInsertContext("insert into t values ('select')");
        Assert.assertTrue(insertValues.isBatchInsert());
        BatchInsertContext insertSelect = new BatchInsertContext("insert into t select * from s");
        Assert.assertFalse(insertSelect.isBatchInsert());
        BatchInsertContext insertOverwrite = new BatchInsertContext("insert overwrite table t values (?)");
        Assert.assertFalse(insertOverwrite.isBatchInsert());
    }
}
