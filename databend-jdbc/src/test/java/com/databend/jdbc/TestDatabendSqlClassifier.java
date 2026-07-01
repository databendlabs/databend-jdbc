package com.databend.jdbc;

import com.databend.jdbc.internal.binding.DatabendSqlClassifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.databend.jdbc.internal.binding.DatabendSqlClassifier.StatementKind.INSERT_VALUES;
import static com.databend.jdbc.internal.binding.DatabendSqlClassifier.StatementKind.OTHER;
import static com.databend.jdbc.internal.binding.DatabendSqlClassifier.StatementKind.REPLACE_VALUES;

public class TestDatabendSqlClassifier {
    @Test(groups = "UNIT")
    public void testInsertValuesClassification() {
        assertKind(INSERT_VALUES, "insert into t values (?, ?)");
        assertKind(INSERT_VALUES, "  /* leading */ INSERT INTO `t` (`a`) VALUES (?) ;");
        assertKind(INSERT_VALUES, "-- comment\ninsert into t values (?)");
        assertKind(INSERT_VALUES, "settings (timezone='Asia/Shanghai') insert into t values (?)");
        assertKind(INSERT_VALUES, "settings (timezone='UTC', max_threads=1) insert into t values (?)");
        assertKind(INSERT_VALUES, "with src as (select 1) insert into t values (?)");
        assertKind(INSERT_VALUES, "insert into t values ('select')");
        assertKind(INSERT_VALUES, "insert into `db`.`tb-test` values (?)");
        assertKind(INSERT_VALUES, "insert into `db`.`tb``name` values (?)");
        assertKind(INSERT_VALUES, "insert into `tb-test` values (?)");
    }

    @Test(groups = "UNIT")
    public void testReplaceValuesClassification() {
        assertKind(REPLACE_VALUES, "replace into t on(id) values (?)");
        assertKind(REPLACE_VALUES, "replace into db.t (a, b) on conflict (a) values (?, ?)");
    }

    @Test(groups = "UNIT")
    public void testNonBatchInsertClassification() {
        assertKind(OTHER, "insert into t select * from s");
        assertKind(OTHER, "insert overwrite table db.t values (?)");
        assertKind(OTHER, "insert into t from @_databend_load file_format=(type=csv)");
        assertKind(OTHER,
                "insert into t values ('a', ?, ?) from @_databend_load file_format=(type=csv)");
        assertKind(OTHER, "insert into db.tb-test values (?)");
        assertKind(OTHER, "select 'insert into t values'");
        assertKind(OTHER, "insert into t values (?); select 1");
    }

    @Test(groups = "UNIT")
    public void testInsertTargetColumnCount() {
        Assert.assertEquals(DatabendSqlClassifier.countInsertTargetColumns("insert into t (a, b) values (?, ?)")
                .get().intValue(), 2);
        Assert.assertEquals(DatabendSqlClassifier.countInsertTargetColumns(
                        "settings (timezone='UTC') insert into t (a, b, c) values")
                .get().intValue(), 3);
        Assert.assertEquals(DatabendSqlClassifier.countInsertTargetColumns(
                        "with src as (select 1) insert into t (a, b) select * from src")
                .get().intValue(), 2);
        Assert.assertFalse(DatabendSqlClassifier.countInsertTargetColumns("insert into t values (1, 2)")
                .isPresent());
    }

    private static void assertKind(DatabendSqlClassifier.StatementKind expected, String sql) {
        Assert.assertEquals(DatabendSqlClassifier.classify(sql).getKind(), expected);
    }
}
