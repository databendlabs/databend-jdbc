package com.databend.jdbc.internal.binding;

import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;

public final class StatementUtil {
    private static final Logger LOG = Logger.getLogger(StatementUtil.class.getName());

    private StatementUtil() {
    }

    /**
     * Returns true if the statement is a query (eg: SELECT, SHOW).
     *
     * @param cleanSql the clean sql (sql statement without comments)
     * @return true if the statement is a query (eg: SELECT, SHOW).
     */
    public static boolean isQuery(String cleanSql) {
        return DatabendSqlClassifier.isQuery(cleanSql);
    }

    public static int getParameterCount(String sql) {
        return getParameterCount(sql, parseToRawStatementWrapper(sql));
    }

    public static int getParameterCount(String sql, RawStatementWrapper rawStatementWrapper) {
        long markerCount = rawStatementWrapper.getTotalParams();
        if (markerCount > 0) {
            return toIntExact(markerCount);
        }
        return DatabendSqlClassifier.countInsertTargetColumns(sql).orElse(0);
    }


    /**
     * Parse sql statement to a {@link RawStatementWrapper}. The method construct
     * the {@link RawStatementWrapper} by splitting it in a list of sub-statements
     * (supports multistatements)
     *
     * @param sql the sql statement
     * @return a list of {@link StatementInfoWrapper}
     */
    public static RawStatementWrapper parseToRawStatementWrapper(String sql) {
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

    private static boolean isEndingSemicolon(char currentChar, char previousChar, boolean foundSubqueryEndingSemicolon,
                                             boolean isPreviousCharInComment) {
        if (foundSubqueryEndingSemicolon) {
            return true;
        }
        return (';' == previousChar && currentChar != ';' && !isPreviousCharInComment);
    }

    private static boolean isEndOfSubquery(char currentChar) {
        return currentChar != '-' && currentChar != '/' && currentChar != ' ' && currentChar != '\n';
    }

    private static boolean isCommentStart(char currentChar) {
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
    public static Map<Integer, Integer> getParamMarketsPositions(String sql) {
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
    public static Pair<Optional<String>, Optional<String>> extractDbNameAndTableNamePairFromCleanQuery(String cleanSql) {
        Optional<String> from = Optional.empty();
        if (isQuery(cleanSql)) {
            LOG.fine("Extracting DB and Table name for SELECT: " + cleanSql);
            String withoutQuotes = StringUtils.replace(cleanSql, "'", "").trim();
            if (StringUtils.startsWithIgnoreCase(withoutQuotes, "select")) {
                int fromIndex = StringUtils.indexOfIgnoreCase(withoutQuotes, "from");
                if (fromIndex != -1) {
                    from = Optional.of(withoutQuotes.substring(fromIndex + "from".length()).trim().split(" ")[0]);
                }
            } else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "DESCRIBE")) {
                from = Optional.of("tables");
            } else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "SHOW")) {
                // Depends on the information requested
                from = Optional.empty();
            } else {
                LOG.fine("Could not find table name for query " + cleanSql + ". This may happen when there is no table.");
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
    public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
                                                                             @NonNull RawStatementWrapper rawStatement) {
        if (params.size() != rawStatement.getTotalParams()) {
            throw new IllegalArgumentException(String.format(
                    "The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: %d, Parameter markers in the SQL query: %d",
                    params.size(), rawStatement.getTotalParams()));
        }

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
            subQueries.add(new StatementInfoWrapper(subQueryWithParams, UUID.randomUUID().toString().replace("-", ""),
                    subQuery.getStatementType(), subQuery));

        }
        return subQueries;
    }

    private static Optional<String> extractTableNameFromFromPartOfTheQuery(String from) {
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

    private static boolean isInSingleLineComment(char currentChar, char previousChar, boolean isCurrentSubstringBetweenQuotes,
                                                 boolean isInSingleLineComment) {
        if (!isCurrentSubstringBetweenQuotes && (previousChar == '-' && currentChar == '-')) {
            return true;
        } else if (currentChar == '\n') {
            return false;
        }
        return isInSingleLineComment;
    }

}
