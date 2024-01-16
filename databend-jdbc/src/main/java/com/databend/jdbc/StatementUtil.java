package com.databend.jdbc;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import lombok.CustomLog;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
@CustomLog
public class StatementUtil {

    private static final String SET_PREFIX = "set";
    private static final Pattern SET_WITH_SPACE_REGEX = Pattern.compile(SET_PREFIX + " ", Pattern.CASE_INSENSITIVE);
    private static final String[] SELECT_KEYWORDS = new String[]{"show", "select", "describe", "exists", "explain",
            "with", "call"};

    /**
     * Returns true if the statement is a query (eg: SELECT, SHOW).
     *
     * @param cleanSql the clean sql (sql statement without comments)
     * @return true if the statement is a query (eg: SELECT, SHOW).
     */
    public static boolean isQuery(String cleanSql) {
        if (StringUtils.isNotEmpty(cleanSql)) {
            cleanSql = cleanSql.replace("(", "");
            return StringUtils.startsWithAny(cleanSql.toLowerCase(), SELECT_KEYWORDS);
        } else {
            return false;
        }
    }

    /**
     * Extracts parameter from statement (eg: SET x=y)
     *
     * @param cleanSql the clean version of the sql (sql statement without comments)
     * @param sql the sql statement
     * @return an optional parameter represented with a pair of key/value
     */
    public Optional<Pair<String, String>> extractParamFromSetStatement(@NonNull String cleanSql, String sql) {
        if (StringUtils.startsWithIgnoreCase(cleanSql, SET_PREFIX)) {
            return extractPropertyPair(cleanSql, sql);
        }
        return Optional.empty();
    }

    /**
     * This method is used to extract column types from a SQL statement.
     * It parses the SQL statement and finds the column types defined in the first pair of parentheses.
     * The column types are then stored in a Map where the key is the index of the column in the SQL statement
     * and the value is the type of the column.
     *
     * @param sql The SQL statement from which to extract column types.
     * @return A Map where the key is the index of the column and the value is the type of the column.
     */
    public static Map<Integer, String> extractColumnTypes(String sql) {
        Map<Integer, String> columnTypes = new LinkedHashMap<>();
        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String[] columns = matcher.group(1).split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i].trim();
                String type = column.substring(column.lastIndexOf(' ') + 1).replace("'", "");
                columnTypes.put(i, type);
            }
        }
        return columnTypes;
    }

    /**
     * Parse the sql statement to a list of {@link StatementInfoWrapper}
     *
     * @param sql the sql statement
     * @return a list of {@link StatementInfoWrapper}
     */
    public List<StatementInfoWrapper> parseToStatementInfoWrappers(String sql) {
        return parseToRawStatementWrapper(sql).getSubStatements().stream().map(StatementInfoWrapper::of)
                .collect(Collectors.toList());
    }

    /**
     * Parse sql statement to a {@link RawStatementWrapper}. The method construct
     * the {@link RawStatementWrapper} by splitting it in a list of sub-statements
     * (supports multistatements)
     *
     * @param sql the sql statement
     * @return a list of {@link StatementInfoWrapper}
     */
    public RawStatementWrapper parseToRawStatementWrapper(String sql) {
        List<RawStatement> subStatements = new ArrayList<>();
        List<ParamMarker> subStatementParamMarkersPositions = new ArrayList<>();
        int subQueryStart = 0;
        int currentIndex = 0;
        char currentChar = sql.charAt(currentIndex);
        StringBuilder cleanedSubQuery = isCommentStart(currentChar) ? new StringBuilder()
                : new StringBuilder(String.valueOf(currentChar));
        boolean isCurrentSubstringBetweenQuotes = currentChar == '\'';
        boolean isCurrentSubstringBetweenDoubleQuotes = currentChar == '"';
        boolean isInSingleLineComment = false;
        boolean isInMultipleLinesComment = false;
        boolean isInComment = false;
        boolean foundSubqueryEndingSemicolon = false;
        char previousChar;
        int subQueryParamsCount = 0;
        boolean isPreviousCharInComment;
        while (currentIndex++ < sql.length() - 1) {
            isPreviousCharInComment = isInComment;
            previousChar = currentChar;
            currentChar = sql.charAt(currentIndex);
            isInSingleLineComment = isInSingleLineComment(currentChar, previousChar, isCurrentSubstringBetweenQuotes,
                    isInSingleLineComment);
            isInMultipleLinesComment = isInMultipleLinesComment(currentChar, previousChar,
                    isCurrentSubstringBetweenQuotes, isInMultipleLinesComment);
            isInComment = isInSingleLineComment || isInMultipleLinesComment;
            if (!isInComment) {
                // Although the ending semicolon may have been found, we need to include any
                // potential comments to the subquery
                if (!isCurrentSubstringBetweenQuotes && isEndingSemicolon(currentChar, previousChar,
                        foundSubqueryEndingSemicolon, isPreviousCharInComment)) {
                    foundSubqueryEndingSemicolon = true;
                    if (isEndOfSubquery(currentChar)) {
                        subStatements.add(RawStatement.of(sql.substring(subQueryStart, currentIndex),
                                subStatementParamMarkersPositions, cleanedSubQuery.toString().trim()));
                        subStatementParamMarkersPositions = new ArrayList<>();
                        subQueryStart = currentIndex;
                        foundSubqueryEndingSemicolon = false;
                        cleanedSubQuery = new StringBuilder();
                    }
                } else if (currentChar == '?' && !isCurrentSubstringBetweenQuotes
                        && !isCurrentSubstringBetweenDoubleQuotes) {
                    subStatementParamMarkersPositions
                            .add(new ParamMarker(++subQueryParamsCount, currentIndex - subQueryStart));
                } else if (currentChar == '\'') {
                    isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
                } else if (currentChar == '"') {
                    isCurrentSubstringBetweenDoubleQuotes = !isCurrentSubstringBetweenDoubleQuotes;
                }
                if (!(isCommentStart(currentChar) && !isCurrentSubstringBetweenQuotes)) {
                    cleanedSubQuery.append(currentChar);
                }
            }
        }
        subStatements.add(RawStatement.of(sql.substring(subQueryStart, currentIndex), subStatementParamMarkersPositions,
                cleanedSubQuery.toString().trim()));
        return new RawStatementWrapper(subStatements);
    }

    private boolean isEndingSemicolon(char currentChar, char previousChar, boolean foundSubqueryEndingSemicolon,
                                      boolean isPreviousCharInComment) {
        if (foundSubqueryEndingSemicolon) {
            return true;
        }
        return (';' == previousChar && currentChar != ';' && !isPreviousCharInComment);
    }

    private boolean isEndOfSubquery(char currentChar) {
        return currentChar != '-' && currentChar != '/' && currentChar != ' ' && currentChar != '\n';
    }

    private boolean isCommentStart(char currentChar) {
        return currentChar == '-' || currentChar == '/';
    }

    private static boolean isInMultipleLinesComment(char currentChar, char previousChar,
                                                    boolean isCurrentSubstringBetweenQuotes, boolean isInMultipleLinesComment) {
        if (!isCurrentSubstringBetweenQuotes && (previousChar == '/' && currentChar == '*')) {
            return true;
        } else if ((previousChar == '*' && currentChar == '/')) {
            return false;
        }
        return isInMultipleLinesComment;
    }

    /**
     * Returns the positions of the params markers
     *
     * @param sql the sql statement
     * @return the positions of the params markers
     */
    public Map<Integer, Integer> getParamMarketsPositions(String sql) {
        RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql);
        return rawStatementWrapper.getSubStatements().stream().map(RawStatement::getParamMarkers)
                .flatMap(Collection::stream).collect(Collectors.toMap(ParamMarker::getId, ParamMarker::getPosition));
    }

    /**
     * Extract the database name and the table name from the cleaned sql query
     *
     * @param cleanSql the clean sql query
     * @return the database name and the table name from the sql query as a pair
     */
    public Pair<Optional<String>, Optional<String>> extractDbNameAndTableNamePairFromCleanQuery(String cleanSql) {
        Optional<String> from = Optional.empty();
        if (isQuery(cleanSql)) {
            log.debug("Extracting DB and Table name for SELECT: {}", cleanSql);
            String withoutQuotes = StringUtils.replace(cleanSql, "'", "").trim();
            if (StringUtils.startsWithIgnoreCase(withoutQuotes, "select")) {
                int fromIndex = StringUtils.indexOfIgnoreCase(withoutQuotes, "from");
                if (fromIndex != -1) {
                    from = Optional.of(withoutQuotes.substring(fromIndex + "from".length()).trim().split(" ")[0]);
                }
            } else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "DESCRIBE")) {
                from = Optional.of("tables");
            } else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "SHOW")) {
                from = Optional.empty(); // Depends on the information requested
            } else {
                log.debug("Could not find table name for query {}. This may happen when there is no table.", cleanSql);
            }
        }
        return new ImmutablePair<>(extractDbNameFromFromPartOfTheQuery(from.orElse(null)),
                extractTableNameFromFromPartOfTheQuery(from.orElse(null)));
    }

    /**
     * Returns a list of {@link StatementInfoWrapper} containing sql statements
     * constructed with the sql statement and the parameters provided
     *
     * @param params the parameters
     * @param sql the sql statement
     * @return a list of sql statements containing the provided parameters
     */
    public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
                                                                             @NonNull String sql) {
        RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql);
        return replaceParameterMarksWithValues(params, rawStatementWrapper);
    }

    /**
     * Returns a list of {@link StatementInfoWrapper} containing sql statements
     * constructed with the {@link RawStatementWrapper} and the parameters provided
     *
     * @param params the parameters
     * @param rawStatement the rawStatement
     * @return a list of sql statements containing the provided parameters
     */
    public List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
                                                                      @NonNull RawStatementWrapper rawStatement) {
        List<StatementInfoWrapper> subQueries = new ArrayList<>();
        for (int subqueryIndex = 0; subqueryIndex < rawStatement.getSubStatements().size(); subqueryIndex++) {
            int currentPos;
            /*
             * As the parameter markers are being placed then the statement sql keeps
             * getting bigger, which is why we need to keep track of the offset
             */
            int offset = 0;
            RawStatement subQuery = rawStatement.getSubStatements().get(subqueryIndex);
            String subQueryWithParams = subQuery.getSql();

            if (params.size() != rawStatement.getTotalParams()) {
                throw new IllegalArgumentException(String.format(
                        "The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: %d, Parameter markers in the SQL query: %d",
                        params.size(), rawStatement.getTotalParams()));
            }
            for (ParamMarker param : subQuery.getParamMarkers()) {
                String value = params.get(param.getId());
                if (value == null) {
                    throw new IllegalArgumentException("No value for parameter marker at position: " + param.getId());
                }
                currentPos = param.getPosition() + offset;
                if (currentPos >= subQuery.getSql().length() + offset) {
                    throw new IllegalArgumentException("The position of the parameter marker provided is invalid");
                }
                subQueryWithParams = subQueryWithParams.substring(0, currentPos) + value
                        + subQueryWithParams.substring(currentPos + 1);
                offset += value.length() - 1;
            }
            Pair<String, String> additionalParams = subQuery.getStatementType() == StatementType.PARAM_SETTING
                    ? ((SetParamRawStatement) subQuery).getAdditionalProperty()
                    : null;
            subQueries.add(new StatementInfoWrapper(subQueryWithParams, UUID.randomUUID().toString(),
                    subQuery.getStatementType(), additionalParams, subQuery));

        }
        return subQueries;
    }

    private Optional<String> extractTableNameFromFromPartOfTheQuery(String from) {
        return Optional.ofNullable(from).map(s -> s.replace("\"", "")).map(fromPartOfTheQuery -> {
            if (StringUtils.contains(fromPartOfTheQuery, ".")) {
                int indexOfTableName = StringUtils.lastIndexOf(fromPartOfTheQuery, ".");
                return fromPartOfTheQuery.substring(indexOfTableName + 1);
            } else {
                return fromPartOfTheQuery;
            }
        });
    }

    private static Optional<String> extractDbNameFromFromPartOfTheQuery(String from) {
        return Optional.ofNullable(from).map(s -> s.replace("\"", ""))
                .filter(s -> StringUtils.countMatches(s, ".") == 2).map(fromPartOfTheQuery -> {
                    int dbNameEndPos = StringUtils.indexOf(fromPartOfTheQuery, ".");
                    return fromPartOfTheQuery.substring(0, dbNameEndPos);
                });
    }

    private boolean isInSingleLineComment(char currentChar, char previousChar, boolean isCurrentSubstringBetweenQuotes,
                                          boolean isInSingleLineComment) {
        if (!isCurrentSubstringBetweenQuotes && (previousChar == '-' && currentChar == '-')) {
            return true;
        } else if (currentChar == '\n') {
            return false;
        }
        return isInSingleLineComment;
    }

    private Optional<Pair<String, String>> extractPropertyPair(String cleanStatement, String sql) {
        String setQuery = RegExUtils.removeFirst(cleanStatement, SET_WITH_SPACE_REGEX);
        String[] values = StringUtils.split(setQuery, "=");
        if (values.length == 2) {
            String value = StringUtils.removeEnd(values[1], ";").trim();
            if (StringUtils.isNumeric(value)) {
                return Optional.of(Pair.of(values[0].trim(), value.trim()));
            } else {
                return Optional.of(Pair.of(values[0].trim(), StringUtils.removeEnd(StringUtils.removeStart(value, "'"), "'")));
            }
        } else {
            throw new IllegalArgumentException(
                    "Cannot parse the additional properties provided in the statement: " + sql);
        }
    }
}
