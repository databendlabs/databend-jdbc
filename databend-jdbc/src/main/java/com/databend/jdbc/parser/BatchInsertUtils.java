package com.databend.jdbc.parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BatchInsertUtils
{
    private final String sql;
    // prepareValues[i] is null if the i-th value is a placeholder

    private TreeMap<Integer, String> placeHolderEntries;
    private BatchInsertUtils(String sql)
    {
        this.sql = sql;
        // sort key in ascending order
        this.placeHolderEntries = new TreeMap<>();
    }

    /**
     * Parse the sql to get insert AST
     * @param sql candidate sql
     * @return BatchInertUtils if the sql is a batch insert sql
     */
    public static Optional<BatchInsertUtils> tryParseInsertSql(String sql)
    {
        return Optional.of(new BatchInsertUtils(sql));
    }

    public String getSql()
    {
        return sql;
    }

    public void setPlaceHolderValue(int index, String value) throws IllegalArgumentException
    {
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
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File tempFile = new File(tempDir, "databend_batch_insert_" + System.currentTimeMillis() + ".csv");
        return saveBatchToCSV(values, tempFile);
    }

    private String convertToCSV(String[] data) {
        return Stream.of(data)
                .collect(Collectors.joining(","));
    }

    public File saveBatchToCSV(List<String[]> values, File file) {
        // save values to csv file
        try (PrintWriter pw = new PrintWriter(file)) {
            values.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
            return file;
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void clean() {
        placeHolderEntries.clear();
    }
}
