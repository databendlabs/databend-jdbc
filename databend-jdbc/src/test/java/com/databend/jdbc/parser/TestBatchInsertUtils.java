package com.databend.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TestBatchInsertUtils
{
    @Test(groups = {"Unit"})
    public void testExpectedValues() {
       HashMap<Integer, String> map = new HashMap<>();
       map.put(1, "a");
        expectValues("insert INTO table1  VALUES ('?', \"asa 12 ??'??' \", 3, 3.0, ?)", map
                , new String[]{"'?'", "\"asa 12 ??'??' \"", "3", "3.0", "a"});
        map.clear();
        map.put(1, "a");
        map.put(2, "b");
        expectValues("insert INTO table1  VALUES ('?', ?, \"asa 12 ??'??' \", 3, 3.0, ?)",
                map, new String[]{"'?'", "a", "\"asa 12 ??'??' \"", "3", "3.0", "b"});
        map.clear();
        map.put(1, "a");
        map.put(2, "12");
        expectValues("insert INTO table1  VALUES ('?', ?, \"asa 12 ??'??' \", 3, 3.0, ?)",
                map, new String[]{"'?'", "a", "\"asa 12 ??'??' \"", "3", "3.0", "12"});
    }

    @Test(groups = {"Unit"})
    public void testExpectedEmpty() {
        expectEmpty("");
        expectEmpty("select * from numbers");
        expectEmpty("insert into numbers values (1,2,3), (3,5,6)");
        expectEmpty("presign a");
    }


    private void expectEmpty(String sql) {
        Optional<BatchInsertUtils> t = BatchInsertUtils.tryParseInsertSql(sql);
        Assert.assertFalse(t.isPresent());
    }

    private void expectValues(String sql, Map<Integer, String> sets, String[] values) {
        Optional<BatchInsertUtils> t = BatchInsertUtils.tryParseInsertSql(sql);
        Assert.assertTrue(t.isPresent());
        for (Map.Entry<Integer, String> entry : sets.entrySet()) {
            t.get().setPlaceHolderValue(entry.getKey(), entry.getValue());
        }
        Assert.assertEquals(t.get().getValues(), values);
    }

}
