package com.databend.jdbc;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.databend.jdbc.StatementUtil.replaceParameterMarksWithValues;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StatementUtilTest {
    @Test
    void shouldGetAllQueryParamsFromIn() {
        String sql = "SElECT * FROM EMPLOYEES WHERE id IN (?,?)";
        assertEquals(ImmutableMap.of(1, 37, 2, 39), StatementUtil.getParamMarketsPositions(sql));
        System.out.println(StatementUtil.parseToRawStatementWrapper(sql).getSubStatements());
        assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
    }

    @Test
    void shouldGetAllQueryParams() {
        String sql = "SElECT * FROM EMPLOYEES WHERE id = ?";
        assertEquals(ImmutableMap.of(1, 35), StatementUtil.getParamMarketsPositions(sql));
        assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
    }

    @Test
    void shouldReplaceAQueryParam() {
        String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
        String expectedSql = "SElECT * FROM EMPLOYEES WHERE id is 5";
        Map<Integer, String> params = ImmutableMap.of(1, "5");
        System.out.println(replaceParameterMarksWithValues(params, sql));
        assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
    }

    @Test
    void shouldReplaceMultipleQueryParams() {
        String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ";
        String expectedSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ";
        Map<Integer, String> params = ImmutableMap.of(1, "5", 2, "'George'", 3, "'1980-05-22'");
        assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
    }
}
