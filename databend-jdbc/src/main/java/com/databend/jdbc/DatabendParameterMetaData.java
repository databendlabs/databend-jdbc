package com.databend.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;

import static com.databend.jdbc.DatabendResultSetMetaData.getTypeClassName;
import static java.util.Objects.requireNonNull;

public class DatabendParameterMetaData extends JdbcWrapper implements ParameterMetaData {
    protected final List<DatabendColumnInfo> params;
    protected final JdbcTypeMapping mapper;

    protected DatabendParameterMetaData(List<DatabendColumnInfo> params, JdbcTypeMapping mapper) {
        this.params = requireNonNull(params, "connection is null");
        this.mapper = mapper;
    }

    protected DatabendColumnInfo getParameter(int param) throws SQLException {
        if (param < 1 || param > params.size()) {
            throw new RuntimeException(format("Parameter index should between 1 and %d but we got %d", params.size(), param));
        }

        return params.get(param - 1);
    }

    public static String format(String template, Object... args) {
        return String.format(Locale.ROOT, template, args);
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        if (p == null) {
            return ParameterMetaData.parameterNullableUnknown;
        }

        return p.getType().isNullable() ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        return p != null && p.isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        return p != null ? p.getPrecision() : 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        return p != null ? p.getScale() : 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        return p != null ? mapper.toSqlType(p) : Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        return p != null ? p.getColumnTypeName() : "<unknown>";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        DatabendColumnInfo p = getParameter(param);
        return getTypeClassName(p.getColumnType());
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
