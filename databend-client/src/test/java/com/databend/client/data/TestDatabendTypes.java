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
}
