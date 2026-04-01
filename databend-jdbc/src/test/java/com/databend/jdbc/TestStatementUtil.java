package com.databend.jdbc;


import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;


public class TestStatementUtil {
    @Test(groups = {"UNIT"})
    public void testExtractColumnTypes() {
        String sql = "insert into non_existing_table ('col2 String, col3 Int8, col1 VARIANT') values (?, ?, ?)";
        Map<Integer, String> columnTypes = StatementUtil.extractColumnTypes(sql);

        assertEquals(3, columnTypes.size());
        assertEquals("String", columnTypes.get(0));
        assertEquals("Int8", columnTypes.get(1));
        assertEquals("VARIANT", columnTypes.get(2));
    }

    @Test(groups = {"UNIT"})
    public void testExtractColumnTypesSelectCountStar() {
        String sql = "SELECT COUNT(*) FROM 'szps_dwd'.'DWD_WSC_WS_GYYX_LJSLLJ_1H'";
        Map<Integer, String> columnTypes = StatementUtil.extractColumnTypes(sql);
        assertEquals(0, columnTypes.size());
    }

    @Test(groups = {"UNIT"})
    public void testExtractColumnTypesSelectWithFunctions() {
        String sql = "SELECT SUM(a), AVG(b) FROM test_table WHERE id = ?";
        Map<Integer, String> columnTypes = StatementUtil.extractColumnTypes(sql);
        assertEquals(0, columnTypes.size());
    }

    @Test(groups = {"UNIT"})
    public void testExtractColumnTypesShowTables() {
        String sql = "SHOW TABLES";
        Map<Integer, String> columnTypes = StatementUtil.extractColumnTypes(sql);
        assertEquals(0, columnTypes.size());
    }
}
