package com.databend.client.data;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestColumnTypeHandlerFactory
{
    @Test(groups = {"Unit"})
    public void testGetTypeFunction() {
        assertTypeHandler("Boolean", BooleanHandler.class, false);
        assertTypeHandler("Nullable(Boolean)", BooleanHandler.class, true);
        assertTypeHandler("UInt8", UInt8Handler.class, false);
        assertTypeHandler("Nullable(Uint8)", UInt8Handler.class, true);
        assertTypeHandler("UInt16", UInt16Handler.class, false);
        assertTypeHandler("Nullable(Uint16)", UInt16Handler.class, true);
        assertTypeHandler("UInt32", UInt32Handler.class, false);
        assertTypeHandler("Nullable(Uint32)", UInt32Handler.class, true);
        assertTypeHandler("UInt64", UInt64Handler.class, false);
        assertTypeHandler("Nullable(Uint64)", UInt64Handler.class, true);
        assertTypeHandler("Int8", Int8Handler.class, false);
        assertTypeHandler("Nullable(Int8)", Int8Handler.class, true);
        assertTypeHandler("Int16", Int16Handler.class, false);
        assertTypeHandler("Nullable(Int16)", Int16Handler.class, true);
        assertTypeHandler("Int32", Int32Handler.class, false);
        assertTypeHandler("Nullable(Int32)", Int32Handler.class, true);
        assertTypeHandler("Int64", Int64Handler.class, false);
        assertTypeHandler("Nullable(Int64)", Int64Handler.class, true);
        assertTypeHandler("Float32", Float32Handler.class, false);
        assertTypeHandler("Nullable(Float32)", Float32Handler.class, true);
        assertTypeHandler("Float64", Float64Handler.class, false);
        assertTypeHandler("Nullable(Float64)", Float64Handler.class, true);
        assertTypeHandler("String", StringHandler.class, false);
        assertTypeHandler("Nullable(String)", StringHandler.class, true);
        assertTypeHandler("Date", StringHandler.class, false);
        assertTypeHandler("Nullable(Date)", StringHandler.class, true);
        assertTypeHandler("DateTime", StringHandler.class, false);
        assertTypeHandler("Nullable(DateTime)", StringHandler.class, true);
        assertTypeHandler("DateTime64", StringHandler.class, false);
        assertTypeHandler("Nullable(DateTime64)", StringHandler.class, true);
        assertTypeHandler("Timestamp", StringHandler.class, false);
        assertTypeHandler("Nullable(Timestamp)", StringHandler.class, true);
        assertTypeHandler("Array(String)", StringHandler.class, false);
        assertTypeHandler("Nullable(Array(Int32))", StringHandler.class, true);
        assertTypeHandler("Struct", StringHandler.class, false);
        assertTypeHandler("Nullable(Struct)", StringHandler.class, true);
        assertTypeHandler("Null", StringHandler.class, false);
        assertTypeHandler("Nullable(Null)", StringHandler.class, true);
        assertTypeHandler("Variant", StringHandler.class, false);
        assertTypeHandler("Nullable(Variant)", StringHandler.class, true);
        assertTypeHandler("VariantArray", StringHandler.class, false);
        assertTypeHandler("Nullable(VariantArray)", StringHandler.class, true);
        assertTypeHandler("UUID", StringHandler.class, false);
        assertTypeHandler("Nullable(UUID)", StringHandler.class, true);
        assertTypeHandler("IPv4", StringHandler.class, false);
        assertTypeHandler("Nullable(IPv4)", StringHandler.class, true);
    }

    private void assertTypeHandler(String typeStr, Class<?> clazz, boolean isNullable) {
        DatabendRawType type = new DatabendRawType(typeStr);
        Assert.assertEquals(ColumnTypeHandlerFactory.getTypeHandler(type).getClass(), clazz);
        Assert.assertEquals(type.isNullable(), isNullable);
    }

}
