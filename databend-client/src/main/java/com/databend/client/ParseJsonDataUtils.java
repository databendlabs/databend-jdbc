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

package com.databend.client;

import com.databend.client.data.ColumnTypeHandler;
import com.databend.client.data.ColumnTypeHandlerFactory;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;


final class ParseJsonDataUtils
{
    private ParseJsonDataUtils() {}
    /**
     * parseRawData is used to convert json data an immutable list of data
     * input QuerySchema: contains column names and types
     * input List<List<Object>> : a list of rows parsed from QueryResponse
     * output Iterable<List<Object>> : convert the input rows into DatabendType and return an immutable list
     */
    public static Iterable<List<Object>> parseRawData(List<QueryRowField> schema, List<List<Object>> data)
    {
        if (data == null || schema == null ) {
            return null;
        }
        ColumnTypeHandler[] typeHandlers = createTypeHandlers(schema);
        // ensure parsed data is thread safe
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builderWithExpectedSize(data.size());
        for (List<Object> row : data) {
            if (row.size() != typeHandlers.length) {
                throw new IllegalArgumentException("row / column does not match schema");
            }
            ArrayList<Object> newRow = new ArrayList<>(typeHandlers.length);
            int column = 0;
            for (Object value : row) {
                if (value != null) {
                    value = typeHandlers[column].parseValue(value);
                }
                newRow.add(value);
                column++;
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    private static ColumnTypeHandler[] createTypeHandlers(List<QueryRowField> schema) {
        int index = 0;
        ColumnTypeHandler[] typeHandlers = new ColumnTypeHandler[schema.size()];
        for (QueryRowField field : schema) {
            typeHandlers[index++] = ColumnTypeHandlerFactory.getTypeHandler(field.getDataType());
        }
        return typeHandlers;
    }
}
