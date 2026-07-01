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
    public void testIsBatchInsert() {
        BatchInsertContext insertValues = new BatchInsertContext("insert into t values ('select')");
        Assert.assertTrue(insertValues.isBatchInsert());
        BatchInsertContext insertSelect = new BatchInsertContext("insert into t select * from s");
        Assert.assertFalse(insertSelect.isBatchInsert());
        BatchInsertContext insertOverwrite = new BatchInsertContext("insert overwrite table t values (?)");
        Assert.assertFalse(insertOverwrite.isBatchInsert());
    }
}
