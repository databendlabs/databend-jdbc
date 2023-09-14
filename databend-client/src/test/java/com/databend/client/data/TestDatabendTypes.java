package com.databend.client.data;

import org.testng.Assert;
import org.testng.annotations.Test;

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
    }
}
