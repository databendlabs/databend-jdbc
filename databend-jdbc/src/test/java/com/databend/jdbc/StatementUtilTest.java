package com.databend.jdbc;

import com.databend.jdbc.internal.binding.RawStatementWrapper;
import com.databend.jdbc.internal.binding.StatementUtil;
import com.databend.jdbc.internal.binding.StatementType;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.databend.jdbc.internal.binding.StatementUtil.replaceParameterMarksWithValues;
import static org.testng.Assert.assertEquals;

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

    @Test
    void shouldNotTreatQuerySettingsWrapperAsSetStatement() {
        String sql = "SETTINGS (timezone='UTC', max_threads=1) INSERT INTO t VALUES (?)";

        RawStatementWrapper wrapper = StatementUtil.parseToRawStatementWrapper(sql);

        assertEquals(1, wrapper.getSubStatements().size());
        assertEquals(1L, wrapper.getTotalParams());
        assertEquals(StatementType.NON_QUERY, wrapper.getSubStatements().get(0).getStatementType());
    }

    @Test
    void shouldTreatSetStatementAsRegularNonQuery() {
        String sql = "SET query_tag='a=b'";

        RawStatementWrapper wrapper = StatementUtil.parseToRawStatementWrapper(sql);

        assertEquals(1, wrapper.getSubStatements().size());
        assertEquals(StatementType.NON_QUERY, wrapper.getSubStatements().get(0).getStatementType());
    }

    @Test
    void shouldDetectQueryByFirstKeywordToken() {
        assertEquals(true, StatementUtil.isQuery("SELECT 1"));
        assertEquals(true, StatementUtil.isQuery("  (select 1)"));
        assertEquals(true, StatementUtil.isQuery("select a-b"));
        assertEquals(true, StatementUtil.isQuery("WITH src AS (SELECT 1) SELECT * FROM src"));
        assertEquals(true, StatementUtil.isQuery("SETTINGS (timezone='UTC') SELECT 1"));
        assertEquals(false, StatementUtil.isQuery("selectx 1"));
        assertEquals(false, StatementUtil.isQuery("settings (timezone='UTC') insert into t values (?)"));
        assertEquals(false, StatementUtil.isQuery("WITH src AS (SELECT 1) INSERT INTO t VALUES (?)"));
        assertEquals(false, StatementUtil.isQuery("insert into t values (?)"));
    }
}
