/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databend.client.data;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Types;

public class TestDatabendTypes {

    @Test(groups = {"Unit"})
    public void testTypeNullable() {
        DatabendRawType nullUnit8 = new DatabendRawType("Nullable(Uint8)");
        Assert.assertEquals(nullUnit8.getType(), "Uint8");
        Assert.assertEquals(nullUnit8.isNullable(), true);

        DatabendRawType nullTuple = new DatabendRawType("Nullable(Tuple(String, Nullable(Int8)))");
        Assert.assertEquals(nullTuple.getDataType().getDisplayName(), "tuple");
        Assert.assertEquals(nullTuple.isNullable(), true);
        Assert.assertTrue(nullTuple.getColumnSize() == 2);

        DatabendRawType map = new DatabendRawType("MAP(STRING, STRING)");
        Assert.assertEquals(map.getDataType().getDisplayName(), "map");
        Assert.assertEquals(map.isNullable(), false);

        DatabendRawType variant = new DatabendRawType("VARIANT");
        Assert.assertEquals(variant.getDataType().getDisplayName(), "variant");

        DatabendRawType geometry = new DatabendRawType("Geometry");
        Assert.assertEquals(geometry.getDataType().getDisplayName(), "geometry");
    }

    // =========================================================================
    // Tests for SQL-standard type aliases returned by Databend >= v1.2.889
    // Prior to v1.2.889, information_schema.columns returned internal names
    // like "Nullable(Int64)". After v1.2.889, it returns SQL-standard names
    // like "bigint".
    // =========================================================================

    /**
     * Verify that getByTypeName resolves both old (v1.2.723) and new (v1.2.889)
     * type names to the same DatabendDataType enum value.
     */
    @Test(groups = {"Unit"})
    public void testOldAndNewTypeNamesResolveToSameType() {
        // Integer types
        Assert.assertEquals(DatabendDataType.getByTypeName("int8"), DatabendDataType.getByTypeName("tinyint"));
        Assert.assertEquals(DatabendDataType.getByTypeName("uint8"), DatabendDataType.getByTypeName("tinyint unsigned"));
        Assert.assertEquals(DatabendDataType.getByTypeName("int16"), DatabendDataType.getByTypeName("smallint"));
        Assert.assertEquals(DatabendDataType.getByTypeName("uint16"), DatabendDataType.getByTypeName("smallint unsigned"));
        Assert.assertEquals(DatabendDataType.getByTypeName("int32"), DatabendDataType.getByTypeName("integer"));
        Assert.assertEquals(DatabendDataType.getByTypeName("uint32"), DatabendDataType.getByTypeName("integer unsigned"));
        Assert.assertEquals(DatabendDataType.getByTypeName("int64"), DatabendDataType.getByTypeName("bigint"));
        Assert.assertEquals(DatabendDataType.getByTypeName("uint64"), DatabendDataType.getByTypeName("bigint unsigned"));

        // Float types
        Assert.assertEquals(DatabendDataType.getByTypeName("float32"), DatabendDataType.getByTypeName("float"));
        Assert.assertEquals(DatabendDataType.getByTypeName("float64"), DatabendDataType.getByTypeName("double"));

        // String / Boolean
        Assert.assertEquals(DatabendDataType.getByTypeName("string"), DatabendDataType.getByTypeName("varchar"));
        Assert.assertEquals(DatabendDataType.getByTypeName("boolean"), DatabendDataType.getByTypeName("bool"));
    }

    /**
     * Verify SQL type codes are correct for new SQL-standard type names.
     * This is the core regression: bigint was returning Types.NULL (0) instead
     * of Types.BIGINT (-5).
     */
    @Test(groups = {"Unit"})
    public void testSqlTypeCodesForNewTypeNames() {
        // Signed integers
        Assert.assertEquals(DatabendDataType.getByTypeName("tinyint").getSqlType(), Types.TINYINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("smallint").getSqlType(), Types.SMALLINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("integer").getSqlType(), Types.INTEGER);
        Assert.assertEquals(DatabendDataType.getByTypeName("bigint").getSqlType(), Types.BIGINT);

        // Unsigned integers
        Assert.assertEquals(DatabendDataType.getByTypeName("tinyint unsigned").getSqlType(), Types.TINYINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("smallint unsigned").getSqlType(), Types.SMALLINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("integer unsigned").getSqlType(), Types.INTEGER);
        Assert.assertEquals(DatabendDataType.getByTypeName("bigint unsigned").getSqlType(), Types.BIGINT);

        // Float types
        Assert.assertEquals(DatabendDataType.getByTypeName("float").getSqlType(), Types.FLOAT);
        Assert.assertEquals(DatabendDataType.getByTypeName("double").getSqlType(), Types.DOUBLE);

        // Other types
        Assert.assertEquals(DatabendDataType.getByTypeName("varchar").getSqlType(), Types.VARCHAR);
        Assert.assertEquals(DatabendDataType.getByTypeName("bool").getSqlType(), Types.BOOLEAN);
    }

    /**
     * Verify SQL type codes are still correct for old internal type names
     * (backward compatibility with v1.2.723).
     */
    @Test(groups = {"Unit"})
    public void testSqlTypeCodesForOldTypeNames() {
        Assert.assertEquals(DatabendDataType.getByTypeName("int8").getSqlType(), Types.TINYINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("int16").getSqlType(), Types.SMALLINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("int32").getSqlType(), Types.INTEGER);
        Assert.assertEquals(DatabendDataType.getByTypeName("int64").getSqlType(), Types.BIGINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("uint8").getSqlType(), Types.TINYINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("uint16").getSqlType(), Types.SMALLINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("uint32").getSqlType(), Types.INTEGER);
        Assert.assertEquals(DatabendDataType.getByTypeName("uint64").getSqlType(), Types.BIGINT);
        Assert.assertEquals(DatabendDataType.getByTypeName("float32").getSqlType(), Types.FLOAT);
        Assert.assertEquals(DatabendDataType.getByTypeName("float64").getSqlType(), Types.DOUBLE);
        Assert.assertEquals(DatabendDataType.getByTypeName("boolean").getSqlType(), Types.BOOLEAN);
        Assert.assertEquals(DatabendDataType.getByTypeName("string").getSqlType(), Types.VARCHAR);
    }

    /**
     * Verify that new type names do NOT resolve to NULL (the original bug).
     */
    @Test(groups = {"Unit"})
    public void testNewTypeNamesDoNotResolveToNull() {
        String[] newTypeNames = {
                "tinyint", "tinyint unsigned",
                "smallint", "smallint unsigned",
                "integer", "integer unsigned",
                "bigint", "bigint unsigned",
                "float", "double",
                "varchar", "bool"
        };
        for (String typeName : newTypeNames) {
            Assert.assertNotEquals(DatabendDataType.getByTypeName(typeName), DatabendDataType.NULL,
                    "Type name '" + typeName + "' should not resolve to NULL");
        }
    }

    /**
     * Verify case-insensitive matching for new SQL-standard type names.
     */
    @Test(groups = {"Unit"})
    public void testCaseInsensitiveNewTypeNames() {
        Assert.assertEquals(DatabendDataType.getByTypeName("BIGINT"), DatabendDataType.INT_64);
        Assert.assertEquals(DatabendDataType.getByTypeName("Bigint"), DatabendDataType.INT_64);
        Assert.assertEquals(DatabendDataType.getByTypeName("bigint"), DatabendDataType.INT_64);
        Assert.assertEquals(DatabendDataType.getByTypeName("TINYINT"), DatabendDataType.INT_8);
        Assert.assertEquals(DatabendDataType.getByTypeName("SMALLINT"), DatabendDataType.INT_16);
        Assert.assertEquals(DatabendDataType.getByTypeName("INTEGER"), DatabendDataType.INT_32);
        Assert.assertEquals(DatabendDataType.getByTypeName("VARCHAR"), DatabendDataType.STRING);
        Assert.assertEquals(DatabendDataType.getByTypeName("DOUBLE"), DatabendDataType.DOUBLE);
        Assert.assertEquals(DatabendDataType.getByTypeName("FLOAT"), DatabendDataType.FLOAT);
        Assert.assertEquals(DatabendDataType.getByTypeName("BOOL"), DatabendDataType.BOOLEAN);
        Assert.assertEquals(DatabendDataType.getByTypeName("BIGINT UNSIGNED"), DatabendDataType.UNSIGNED_INT_64);
    }

    /**
     * Verify DatabendRawType correctly handles Nullable wrapper with new type names.
     * Simulates: Databend v1.2.889 returning "Nullable(bigint)" instead of "Nullable(Int64)".
     */
    @Test(groups = {"Unit"})
    public void testNullableWithNewTypeNames() {
        // Nullable signed integers
        DatabendRawType nullBigint = new DatabendRawType("Nullable(bigint)");
        Assert.assertTrue(nullBigint.isNullable());
        Assert.assertEquals(nullBigint.getDataType(), DatabendDataType.INT_64);
        Assert.assertEquals(nullBigint.getDataType().getSqlType(), Types.BIGINT);

        DatabendRawType nullTinyint = new DatabendRawType("Nullable(tinyint)");
        Assert.assertTrue(nullTinyint.isNullable());
        Assert.assertEquals(nullTinyint.getDataType(), DatabendDataType.INT_8);
        Assert.assertEquals(nullTinyint.getDataType().getSqlType(), Types.TINYINT);

        DatabendRawType nullSmallint = new DatabendRawType("Nullable(smallint)");
        Assert.assertTrue(nullSmallint.isNullable());
        Assert.assertEquals(nullSmallint.getDataType(), DatabendDataType.INT_16);
        Assert.assertEquals(nullSmallint.getDataType().getSqlType(), Types.SMALLINT);

        DatabendRawType nullInteger = new DatabendRawType("Nullable(integer)");
        Assert.assertTrue(nullInteger.isNullable());
        Assert.assertEquals(nullInteger.getDataType(), DatabendDataType.INT_32);
        Assert.assertEquals(nullInteger.getDataType().getSqlType(), Types.INTEGER);

        // Nullable float types
        DatabendRawType nullFloat = new DatabendRawType("Nullable(float)");
        Assert.assertTrue(nullFloat.isNullable());
        Assert.assertEquals(nullFloat.getDataType(), DatabendDataType.FLOAT);
        Assert.assertEquals(nullFloat.getDataType().getSqlType(), Types.FLOAT);

        DatabendRawType nullDouble = new DatabendRawType("Nullable(double)");
        Assert.assertTrue(nullDouble.isNullable());
        Assert.assertEquals(nullDouble.getDataType(), DatabendDataType.DOUBLE);
        Assert.assertEquals(nullDouble.getDataType().getSqlType(), Types.DOUBLE);

        // Nullable string / boolean
        DatabendRawType nullVarchar = new DatabendRawType("Nullable(varchar)");
        Assert.assertTrue(nullVarchar.isNullable());
        Assert.assertEquals(nullVarchar.getDataType(), DatabendDataType.STRING);

        DatabendRawType nullBool = new DatabendRawType("Nullable(bool)");
        Assert.assertTrue(nullBool.isNullable());
        Assert.assertEquals(nullBool.getDataType(), DatabendDataType.BOOLEAN);
    }

    /**
     * Verify non-nullable new type names work correctly.
     * Simulates: Databend v1.2.889 returning "bigint" instead of "Int64".
     */
    @Test(groups = {"Unit"})
    public void testNonNullableNewTypeNames() {
        DatabendRawType bigint = new DatabendRawType("bigint");
        Assert.assertFalse(bigint.isNullable());
        Assert.assertEquals(bigint.getDataType(), DatabendDataType.INT_64);
        Assert.assertEquals(bigint.getDataType().getSqlType(), Types.BIGINT);

        DatabendRawType tinyint = new DatabendRawType("tinyint");
        Assert.assertFalse(tinyint.isNullable());
        Assert.assertEquals(tinyint.getDataType(), DatabendDataType.INT_8);

        DatabendRawType varchar = new DatabendRawType("varchar");
        Assert.assertFalse(varchar.isNullable());
        Assert.assertEquals(varchar.getDataType(), DatabendDataType.STRING);
    }

    /**
     * Verify signed/unsigned properties are correct for new type names.
     */
    @Test(groups = {"Unit"})
    public void testSignedPropertyForNewTypeNames() {
        Assert.assertTrue(DatabendDataType.getByTypeName("bigint").isSigned());
        Assert.assertFalse(DatabendDataType.getByTypeName("bigint unsigned").isSigned());
        Assert.assertTrue(DatabendDataType.getByTypeName("tinyint").isSigned());
        Assert.assertFalse(DatabendDataType.getByTypeName("tinyint unsigned").isSigned());
        Assert.assertTrue(DatabendDataType.getByTypeName("smallint").isSigned());
        Assert.assertFalse(DatabendDataType.getByTypeName("smallint unsigned").isSigned());
        Assert.assertTrue(DatabendDataType.getByTypeName("integer").isSigned());
        Assert.assertFalse(DatabendDataType.getByTypeName("integer unsigned").isSigned());
    }

    /**
     * Simulate the exact scenario from the bug report:
     * DatabaseMetaData.getColumns() returns TYPE_NAME=bigint, and we expect
     * DATA_TYPE=Types.BIGINT (-5), not 0.
     */
    @Test(groups = {"Unit"})
    public void testBugScenario_BigintSqlTypeCode() {
        // Before fix: getByTypeName("bigint") returned NULL, getSqlType() = Types.NULL = 0
        // After fix: getByTypeName("bigint") returns INT_64, getSqlType() = Types.BIGINT = -5
        DatabendDataType dataType = DatabendDataType.getByTypeName("bigint");
        Assert.assertEquals(dataType, DatabendDataType.INT_64, "bigint should map to INT_64");
        Assert.assertEquals(dataType.getSqlType(), Types.BIGINT, "bigint SQL type should be Types.BIGINT (-5)");
        Assert.assertNotEquals(dataType.getSqlType(), 0, "bigint SQL type must not be 0 (Types.NULL)");
    }

    /**
     * Verify that old-style Nullable type names still work (backward compat with v1.2.723).
     */
    @Test(groups = {"Unit"})
    public void testOldNullableTypeNamesStillWork() {
        DatabendRawType nullInt64 = new DatabendRawType("Nullable(Int64)");
        Assert.assertTrue(nullInt64.isNullable());
        Assert.assertEquals(nullInt64.getDataType(), DatabendDataType.INT_64);
        Assert.assertEquals(nullInt64.getDataType().getSqlType(), Types.BIGINT);

        DatabendRawType nullInt32 = new DatabendRawType("Nullable(Int32)");
        Assert.assertTrue(nullInt32.isNullable());
        Assert.assertEquals(nullInt32.getDataType(), DatabendDataType.INT_32);

        DatabendRawType nullUint64 = new DatabendRawType("Nullable(Uint64)");
        Assert.assertTrue(nullUint64.isNullable());
        Assert.assertEquals(nullUint64.getDataType(), DatabendDataType.UNSIGNED_INT_64);

        DatabendRawType nullFloat64 = new DatabendRawType("Nullable(Float64)");
        Assert.assertTrue(nullFloat64.isNullable());
        Assert.assertEquals(nullFloat64.getDataType(), DatabendDataType.DOUBLE);
    }
}
