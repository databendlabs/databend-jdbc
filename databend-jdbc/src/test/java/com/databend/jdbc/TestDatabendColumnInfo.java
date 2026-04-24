package com.databend.jdbc;

import com.databend.jdbc.internal.data.DatabendDataType;
import com.databend.jdbc.internal.data.DatabendRawType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Types;

/**
 * Unit tests for DatabendColumnInfo verifying that SQL-standard type names
 * returned by Databend >= v1.2.889 produce correct column metadata.
 *
 * Before v1.2.889, information_schema.columns returned internal names like
 * "Nullable(Int64)". After v1.2.889, it returns SQL-standard names like "bigint".
 */
public class TestDatabendColumnInfo {

    // =========================================================================
    // End-to-end: new type name -> DatabendColumnInfo -> correct metadata
    // =========================================================================

    @Test(groups = {"Unit"})
    public void testBigintColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("bigint"));
        Assert.assertEquals(info.getColumnType(), Types.BIGINT);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 19);
        Assert.assertEquals(info.getColumnDisplaySize(), 20);
        Assert.assertEquals(info.getScale(), 0);
    }

    @Test(groups = {"Unit"})
    public void testNullableBigintColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("Nullable(bigint)"));
        Assert.assertEquals(info.getColumnType(), Types.BIGINT);
        Assert.assertEquals(info.getNullable(), DatabendColumnInfo.Nullable.NULLABLE);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 19);
    }

    @Test(groups = {"Unit"})
    public void testBigintUnsignedColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("bigint unsigned"));
        Assert.assertEquals(info.getColumnType(), Types.BIGINT);
        Assert.assertFalse(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 19);
        Assert.assertEquals(info.getColumnDisplaySize(), 20);
    }

    @Test(groups = {"Unit"})
    public void testTinyintColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("tinyint"));
        Assert.assertEquals(info.getColumnType(), Types.TINYINT);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 4);
        Assert.assertEquals(info.getColumnDisplaySize(), 5);
        Assert.assertEquals(info.getScale(), 0);
    }

    @Test(groups = {"Unit"})
    public void testTinyintUnsignedColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("tinyint unsigned"));
        Assert.assertEquals(info.getColumnType(), Types.TINYINT);
        Assert.assertFalse(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 3);
        Assert.assertEquals(info.getColumnDisplaySize(), 4);
    }

    @Test(groups = {"Unit"})
    public void testSmallintColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("smallint"));
        Assert.assertEquals(info.getColumnType(), Types.SMALLINT);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 5);
        Assert.assertEquals(info.getColumnDisplaySize(), 6);
    }

    @Test(groups = {"Unit"})
    public void testSmallintUnsignedColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("smallint unsigned"));
        Assert.assertEquals(info.getColumnType(), Types.SMALLINT);
        Assert.assertFalse(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 5);
        Assert.assertEquals(info.getColumnDisplaySize(), 6);
    }

    @Test(groups = {"Unit"})
    public void testIntegerColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("integer"));
        Assert.assertEquals(info.getColumnType(), Types.INTEGER);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 10);
        Assert.assertEquals(info.getColumnDisplaySize(), 11);
    }

    @Test(groups = {"Unit"})
    public void testIntegerUnsignedColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("integer unsigned"));
        Assert.assertEquals(info.getColumnType(), Types.INTEGER);
        Assert.assertFalse(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 10);
        Assert.assertEquals(info.getColumnDisplaySize(), 11);
    }

    @Test(groups = {"Unit"})
    public void testFloatColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("float"));
        Assert.assertEquals(info.getColumnType(), Types.FLOAT);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 9);
        Assert.assertEquals(info.getColumnDisplaySize(), 16);
    }

    @Test(groups = {"Unit"})
    public void testDoubleColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("double"));
        Assert.assertEquals(info.getColumnType(), Types.DOUBLE);
        Assert.assertTrue(info.isSigned());
        Assert.assertEquals(info.getPrecision(), 17);
        Assert.assertEquals(info.getColumnDisplaySize(), 24);
    }

    @Test(groups = {"Unit"})
    public void testVarcharColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("varchar"));
        Assert.assertEquals(info.getColumnType(), Types.VARCHAR);
        Assert.assertFalse(info.isSigned());
    }

    @Test(groups = {"Unit"})
    public void testBoolColumnInfo() {
        DatabendColumnInfo info = DatabendColumnInfo.of("col", new DatabendRawType("bool"));
        Assert.assertEquals(info.getColumnType(), Types.BOOLEAN);
        Assert.assertEquals(info.getColumnDisplaySize(), 5);
    }

    // =========================================================================
    // Verify new type names produce identical metadata to old type names
    // =========================================================================

    @Test(groups = {"Unit"})
    public void testNewTypeNamesMatchOldTypeNamesMetadata() {
        // bigint vs Int64
        assertSameMetadata("bigint", "Int64");
        // tinyint vs Int8
        assertSameMetadata("tinyint", "Int8");
        // smallint vs Int16
        assertSameMetadata("smallint", "Int16");
        // integer vs Int32
        assertSameMetadata("integer", "Int32");
        // bigint unsigned vs UInt64
        assertSameMetadata("bigint unsigned", "UInt64");
        // tinyint unsigned vs UInt8
        assertSameMetadata("tinyint unsigned", "UInt8");
        // smallint unsigned vs UInt16
        assertSameMetadata("smallint unsigned", "UInt16");
        // integer unsigned vs UInt32
        assertSameMetadata("integer unsigned", "UInt32");
        // float vs Float32
        assertSameMetadata("float", "Float32");
        // double vs Float64
        assertSameMetadata("double", "Float64");
        // varchar vs String
        assertSameMetadata("varchar", "String");
        // bool vs Boolean
        assertSameMetadata("bool", "Boolean");
    }

    @Test(groups = {"Unit"})
    public void testNullableNewTypeNamesMatchOldTypeNamesMetadata() {
        assertSameMetadata("Nullable(bigint)", "Nullable(Int64)");
        assertSameMetadata("Nullable(tinyint)", "Nullable(Int8)");
        assertSameMetadata("Nullable(smallint)", "Nullable(Int16)");
        assertSameMetadata("Nullable(integer)", "Nullable(Int32)");
        assertSameMetadata("Nullable(float)", "Nullable(Float32)");
        assertSameMetadata("Nullable(double)", "Nullable(Float64)");
        assertSameMetadata("Nullable(varchar)", "Nullable(String)");
        assertSameMetadata("Nullable(bool)", "Nullable(Boolean)");
    }

    // =========================================================================
    // JdbcTypeMapping end-to-end with new type names
    // =========================================================================

    @Test(groups = {"Unit"})
    public void testJdbcTypeMappingWithNewTypeNames() {
        JdbcTypeMapping mapping = new JdbcTypeMapping();

        DatabendColumnInfo bigintCol = DatabendColumnInfo.of("col", new DatabendRawType("bigint"));
        Assert.assertEquals(mapping.toSqlType(bigintCol), Types.BIGINT);

        DatabendColumnInfo tinyintCol = DatabendColumnInfo.of("col", new DatabendRawType("tinyint"));
        Assert.assertEquals(mapping.toSqlType(tinyintCol), Types.TINYINT);

        DatabendColumnInfo smallintCol = DatabendColumnInfo.of("col", new DatabendRawType("smallint"));
        Assert.assertEquals(mapping.toSqlType(smallintCol), Types.SMALLINT);

        DatabendColumnInfo integerCol = DatabendColumnInfo.of("col", new DatabendRawType("integer"));
        Assert.assertEquals(mapping.toSqlType(integerCol), Types.INTEGER);

        DatabendColumnInfo floatCol = DatabendColumnInfo.of("col", new DatabendRawType("float"));
        Assert.assertEquals(mapping.toSqlType(floatCol), Types.FLOAT);

        DatabendColumnInfo doubleCol = DatabendColumnInfo.of("col", new DatabendRawType("double"));
        Assert.assertEquals(mapping.toSqlType(doubleCol), Types.DOUBLE);

        DatabendColumnInfo varcharCol = DatabendColumnInfo.of("col", new DatabendRawType("varchar"));
        Assert.assertEquals(mapping.toSqlType(varcharCol), Types.VARCHAR);

        DatabendColumnInfo boolCol = DatabendColumnInfo.of("col", new DatabendRawType("bool"));
        Assert.assertEquals(mapping.toSqlType(boolCol), Types.BOOLEAN);
    }

    private void assertSameMetadata(String newTypeName, String oldTypeName) {
        DatabendColumnInfo newInfo = DatabendColumnInfo.of("col", new DatabendRawType(newTypeName));
        DatabendColumnInfo oldInfo = DatabendColumnInfo.of("col", new DatabendRawType(oldTypeName));

        Assert.assertEquals(newInfo.getColumnType(), oldInfo.getColumnType(),
                newTypeName + " vs " + oldTypeName + ": columnType mismatch");
        Assert.assertEquals(newInfo.isSigned(), oldInfo.isSigned(),
                newTypeName + " vs " + oldTypeName + ": signed mismatch");
        Assert.assertEquals(newInfo.getPrecision(), oldInfo.getPrecision(),
                newTypeName + " vs " + oldTypeName + ": precision mismatch");
        Assert.assertEquals(newInfo.getScale(), oldInfo.getScale(),
                newTypeName + " vs " + oldTypeName + ": scale mismatch");
        Assert.assertEquals(newInfo.getColumnDisplaySize(), oldInfo.getColumnDisplaySize(),
                newTypeName + " vs " + oldTypeName + ": columnDisplaySize mismatch");
    }
}
