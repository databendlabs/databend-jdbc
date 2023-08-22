package com.databend.jdbc.metadata;

import com.databend.jdbc.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MetadataUtil {
    public String getColumnsQuery(String schemaPattern, String tableNamePattern, String columnNamePattern) {
        Query.QueryBuilder queryBuilder = Query.builder().select(
                        "table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position")
                .from("information_schema.columns");

        List<String> conditions = new ArrayList<>();
        Optional.ofNullable(tableNamePattern)
                .ifPresent(pattern -> conditions.add(String.format("table_name LIKE '%s'", pattern)));
        Optional.ofNullable(columnNamePattern)
                .ifPresent(pattern -> conditions.add(String.format("column_name LIKE '%s'", pattern)));
        Optional.ofNullable(schemaPattern)
                .ifPresent(pattern -> conditions.add(String.format("table_schema LIKE '%s'", pattern)));
        return queryBuilder.conditions(conditions).build().toSql();
    }

}
