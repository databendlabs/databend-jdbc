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

import java.util.Locale;

public class ColumnTypeHandlerFactory
{
    public static ColumnTypeHandler getTypeHandler(DatabendRawType type) {
        if (type == null) {
            return null;
        }
        switch (type.getType().toLowerCase(Locale.US)) {
            case DatabendTypes.INT8:
                return new Int8Handler(type.isNullable());
            case DatabendTypes.INT16:
                return new Int16Handler(type.isNullable());
            case DatabendTypes.INT32:
                return new Int32Handler(type.isNullable());
            case DatabendTypes.INT64:
                return new Int64Handler(type.isNullable());
            case DatabendTypes.UINT8:
                return new UInt8Handler(type.isNullable());
            case DatabendTypes.UINT16:
                return new UInt16Handler(type.isNullable());
            case DatabendTypes.UINT32:
                return new UInt32Handler(type.isNullable());
            case DatabendTypes.UINT64:
                return new UInt64Handler(type.isNullable());
            case DatabendTypes.FLOAT32:
                return new Float32Handler(type.isNullable());
            case DatabendTypes.FLOAT64:
                return new Float64Handler(type.isNullable());
            case DatabendTypes.BOOLEAN:
                return new BooleanHandler(type.isNullable());
            case DatabendTypes.ARRAY:
            case DatabendTypes.DATE:
            case DatabendTypes.DATETIME:
            case DatabendTypes.DATETIME64:
            case DatabendTypes.TIMESTAMP:
            case DatabendTypes.STRING:
            case DatabendTypes.NULL:
            case DatabendTypes.STRUCT:

            case DatabendTypes.VARIANT:
            case DatabendTypes.VARIANT_ARRAY:
            case DatabendTypes.VARIANT_OBJECT:
            case DatabendTypes.INTERVAL:
            default:
                return new StringHandler(type.isNullable());
        }
    }
}
