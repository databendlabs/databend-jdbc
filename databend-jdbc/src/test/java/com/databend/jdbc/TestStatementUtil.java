package com.databend.jdbc;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStatementUtil {
    @Test
    public void testExtractColumnTypes() {
        String sql = "insert into non_existing_table ('col2 String, col3 Int8, col1 VARIANT') values (?, ?, ?)";
        Map<Integer, String> columnTypes = StatementUtil.extractColumnTypes(sql);

        assertEquals(3, columnTypes.size());
        assertEquals("String", columnTypes.get(0));
        assertEquals("Int8", columnTypes.get(1));
        assertEquals("VARIANT", columnTypes.get(2));
    }
}
