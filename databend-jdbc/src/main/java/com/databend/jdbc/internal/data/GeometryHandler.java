package com.databend.jdbc.internal.data;

public class GeometryHandler extends ColumnTypeHandlerBase {
    public GeometryHandler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        if (value.startsWith("00") || value.startsWith("01")) {
            return hexStringToByteArray(value);
        }
        return value;
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
