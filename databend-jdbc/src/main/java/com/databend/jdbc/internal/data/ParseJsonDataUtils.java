package com.databend.jdbc.internal.data;

import com.databend.jdbc.internal.query.QueryRowField;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public final class ParseJsonDataUtils {
    private ParseJsonDataUtils() {
    }

    public static List<List<Object>> parseRawData(List<QueryRowField> schema, List<List<String>> data) {
        if (data == null || schema == null) {
            return null;
        }
        ColumnTypeHandler[] typeHandlers = createTypeHandlers(schema);
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builderWithExpectedSize(data.size());
        for (List<String> row : data) {
            if (row.size() != typeHandlers.length) {
                throw new IllegalArgumentException("row / column does not match schema");
            }
            ArrayList<Object> newRow = new ArrayList<>(typeHandlers.length);
            int column = 0;
            for (String value : row) {
                Object parsed = null;
                if (value != null) {
                    try {
                        parsed = typeHandlers[column].parseString(value);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("fail to parse column " + column + "(" + schema.get(column).getName() + "):" + e.getMessage());
                    }
                }
                newRow.add(parsed);
                column++;
            }
            rows.add(unmodifiableList(newRow));
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
