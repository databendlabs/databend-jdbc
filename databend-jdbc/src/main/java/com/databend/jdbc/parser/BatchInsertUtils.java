package com.databend.jdbc.parser;

import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;
import de.siegmar.fastcsv.writer.QuoteStrategy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BatchInsertUtils {
    private static final Logger logger = Logger.getLogger(BatchInsertUtils.class.getPackage().getName());

    private final String sql;

    private String databaseTableName;
    // prepareValues[i] is null if the i-th value is a placeholder

    private TreeMap<Integer, String> placeHolderEntries;

    private BatchInsertUtils(String sql) {
        this.sql = sql;
        // sort key in ascending order
        this.placeHolderEntries = new TreeMap<>();
//        this.databaseTableName = getDatabaseTableName();
    }

    /**
     * Parse the sql to get insert AST
     *
     * @param sql candidate sql
     * @return BatchInertUtils if the sql is a batch insert sql
     */
    public static Optional<BatchInsertUtils> tryParseInsertSql(String sql) {
        return Optional.of(new BatchInsertUtils(sql));
    }

    public String getSql() {
        return sql;
    }

    public Map<Integer, String> getProvideParams() {
        Map<Integer, String> m = new TreeMap<>();
        for (Map.Entry<Integer, String> elem : placeHolderEntries.entrySet()) {
            m.put(elem.getKey() + 1, elem.getValue());
        }
        return m;
    }

    public String getDatabaseTableName() {
        Pattern pattern = Pattern.compile("^INSERT INTO\\s+((?:[\\w-]+\\.)?([\\w-]+))(?:\\s*\\((?:[^()]|\\([^()]*\\))*\\))?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql.replace("`", ""));

        if (matcher.find()) {
            databaseTableName = matcher.group(1);
        }

        return databaseTableName;
    }

    public void setPlaceHolderValue(int index, String value) throws IllegalArgumentException {
        int i = index - 1;

        placeHolderEntries.put(i, value);
    }

    // get the sql with placeholder replaced by value
    public String[] getValues() {
        if (placeHolderEntries.isEmpty()) {
            return null;
        }
        String[] values = new String[placeHolderEntries.lastKey() + 1];
        for (Map.Entry<Integer, String> elem : placeHolderEntries.entrySet()) {
            values[elem.getKey()] = elem.getValue();
        }
        return values;
    }

    public File saveBatchToCSV(List<String[]> values) {
        // get a temporary directory
        String id = UUID.randomUUID().toString();
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File tempFile = new File(tempDir, "databend_batch_insert_" + id + ".csv");
        return saveBatchToCSV(values, tempFile);
    }

    private String convertToCSV(String[] data) {
        return Stream.of(data)
                .collect(Collectors.joining(","));
    }

    public File saveBatchToCSV(List<String[]> values, File file) {
        int rowSize = 0;
        for (String[] row : values) {
            if (row != null) {
                rowSize = row.length;
                break;
            }
            throw new RuntimeException("batch values is empty");
        }
        // save values to csv file
        try (FileWriter pw = new FileWriter(file)) {
            CsvWriter w = CsvWriter.builder().quoteCharacter('"').lineDelimiter(LineDelimiter.LF).build(pw);
            for (String[] row : values) {
                w.writeRow(row);
            }
            logger.log(Level.FINE, "save batch insert to csv file: " + file.getAbsolutePath() + "rows: " + values.size() + " columns: " + rowSize);
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void clean() {
        placeHolderEntries.clear();
    }
}
