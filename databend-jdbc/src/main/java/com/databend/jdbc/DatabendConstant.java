package com.databend.jdbc;

import java.util.regex.Pattern;

/**
 * databend constant
 *
 * @author wayne
 */
class DatabendConstant {
    public static final String ENABLE_STR = "enable";
    public static final String BASE64_STR = "base64";
    public static final Pattern INSERT_INTO_PATTERN = Pattern.compile("(insert|replace)\\s+into");

    public static final String DATABEND_KEYWORDS_SELECT = "select";
}
