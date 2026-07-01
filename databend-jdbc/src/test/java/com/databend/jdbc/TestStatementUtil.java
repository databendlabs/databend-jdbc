package com.databend.jdbc;


import com.databend.jdbc.internal.binding.StatementUtil;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TestStatementUtil {
    @Test(groups = {"UNIT"})
    public void testParameterCountUsesMarkersFirst() {
        String sql = "insert into non_existing_table (col2, col3, col1) values (?, ?, ?)";

        assertEquals(3, StatementUtil.getParameterCount(sql));
    }

    @Test(groups = {"UNIT"})
    public void testParameterCountFallsBackToInsertTargetColumns() {
        String sql = "insert into non_existing_table (col2, col3, col1) values";

        assertEquals(3, StatementUtil.getParameterCount(sql));
    }

    @Test(groups = {"UNIT"})
    public void testParameterCountDoesNotUseValuesTupleAsFallback() {
        String sql = "insert into non_existing_table values (1, 2, 3)";

        assertEquals(0, StatementUtil.getParameterCount(sql));
    }

    @Test(groups = {"UNIT"})
    public void testParameterCountSelectCountStar() {
        String sql = "SELECT COUNT(*) FROM 'szps_dwd'.'DWD_WSC_WS_GYYX_LJSLLJ_1H'";
        assertEquals(0, StatementUtil.getParameterCount(sql));
    }

    @Test(groups = {"UNIT"})
    public void testParameterCountSelectWithFunctions() {
        String sql = "SELECT SUM(a), AVG(b) FROM test_table WHERE id = ?";
        assertEquals(1, StatementUtil.getParameterCount(sql));
    }

    @Test(groups = {"UNIT"})
    public void testParameterCountShowTables() {
        String sql = "SHOW TABLES";
        assertEquals(0, StatementUtil.getParameterCount(sql));
    }
}
