package com.databend.jdbc.cloud;

import javax.xml.crypto.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

public class DatabendCopyParams
{
    private static final String defaultType = "CSV";
    private final List<String> files;
    private final String pattern;

    private final String type;
    private final Map<String, String> fileOptions;
    private final Map<String, String> copyOptions;

    private DatabendCopyParams(List<String> files, String pattern, String type, Map<String, String> fileOptions, Map<String, String> copyOptions)
    {
        this.files = files;
        this.pattern = pattern;
        if (type !=null) {
            this.type = type;
        } else {
            this.type = defaultType;
        }
        this.fileOptions = fileOptions;
        this.copyOptions = copyOptions;
    }
    public List<String> getFiles()
    {
        return files;
    }

    public String getPattern()
    {
        return pattern;
    }

    public String getType()
    {
        return type;
    }

    public Map<String, String> getFileOptions()
    {
        return fileOptions;
    }

    public Map<String, String> getCopyOptions()
    {
        return copyOptions;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (this.files != null && !this.files.isEmpty()) {
            StringJoiner s = new StringJoiner(",");
            for (String file : this.files) {
                s.add("'" + file + "'");
            }
            sb.append("FILES = ");
            sb.append("(");
            sb.append(s);
            sb.append(")");
            sb.append(" ");
        }

        if (this.pattern != null) {
            sb.append("PATTERN = ");
            sb.append("'").append(this.pattern).append("'").append(" ");
        }
        sb.append("FILE_FORMAT = ( ");
        sb.append("type = ");
        sb.append("'").append(this.type).append("'").append(" ");
        if (this.fileOptions != null) {
            for (Map.Entry<String, String> e : this.fileOptions.entrySet()) {
                parseParam(e, sb);
            }
        }
        sb.append(")").append(" ");
        if (this.copyOptions != null) {
            for (Map.Entry<String, String> e : this.copyOptions.entrySet()) {
                parseParam(e, sb);
            }
        }
        return sb.toString();
    }

    private static void parseParam(Map.Entry<String, String> s, StringBuilder sb) {
        boolean needQuote = true;
        try {
            DatabendParams p = DatabendParams.valueOf(s.getKey().toUpperCase(Locale.US));
            needQuote = p.needQuote();
        } catch (IllegalArgumentException e) {
        }
        if (needQuote) {
            sb.append(s.getKey()).append(" = ").append("'").append(s.getValue()).append("'").append(" ");
        } else {
            sb.append(s.getKey()).append(" = ").append(s.getValue()).append(" ");
        }
    }

    public static DatabendCopyParams.Builder builder()
    {
        return new DatabendCopyParams.Builder();
    }
    public static class Builder
    {
        private List<String> files;
        private  String pattern;

        private  String type;
        private  Map<String, String> fileOptions;
        private  Map<String, String> copyOptions;

        public Builder setFiles(List<String> files) {
            this.files = files;
            return this;
        }
        public Builder setPattern(String pattern)
        {
            this.pattern = pattern;
            return this;
        }

        public Builder setType(String type)
        {
            this.type = type;
            return this;
        }

        public Builder setFileOptions(Map<String, String> fileOptions)
        {
            this.fileOptions = fileOptions;
            return this;
        }

        public Builder setCopyOptions(Map<String, String> copyOptions)
        {
            this.copyOptions = copyOptions;
            return this;
        }

        public DatabendCopyParams build()
        {
            return new DatabendCopyParams(files, pattern, type, fileOptions, copyOptions);
        }
    }

    public enum DatabendParams {
        RECORD_DELIMITER("RECORD_DELIMITER", String.class),
        FIELD_DELIMITER("FIELD_DELIMITER", String.class),
        SKIP_HEADER("SKIP_HEADER", Integer.class),
        QUOTE("QUOTE", String.class),
        ESCAPE("ESCAPE", String.class),
        NAN_DISPLAY("NAN_DISPLAY", String.class),
        ROW_TAG("ROW_TAG", String.class),
        COMPRESSION("COMPRESSION", String.class),
        SIZE_LIMIT("SIZE_LIMIT", Integer.class),
        PURGE("PURGE", Boolean.class),
        FORCE("FORCE", Boolean.class),
        // on error only support continue/abort without quote
        ON_ERROR("ON_ERROR", null);


        private final String name;
        private final Class<?> type;
        DatabendParams(String name, Class<?> type)
        {
            this.name = name;
            this.type = type;
        }
        public boolean needQuote()
        {
            if (type == null) {
                return false;
            }
            return type == String.class;
        }
    }
}
