package com.databend.jdbc.internal.binding;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class DatabendSqlClassifier {
    public enum StatementKind {
        INSERT_VALUES,
        REPLACE_VALUES,
        INSERT_SELECT_OR_LOAD,
        OTHER
    }

    public static final class Classification {
        private final StatementKind kind;
        private final String tableName;

        private Classification(StatementKind kind, String tableName) {
            this.kind = kind;
            this.tableName = tableName;
        }

        public StatementKind getKind() {
            return kind;
        }

        public Optional<String> getTableName() {
            return Optional.ofNullable(tableName);
        }

        public boolean isBatchInsert() {
            return kind == StatementKind.INSERT_VALUES || kind == StatementKind.REPLACE_VALUES;
        }
    }

    private enum TokenKind {
        WORD,
        QUOTED_IDENTIFIER,
        STRING,
        SYMBOL
    }

    private static final Classification OTHER = new Classification(StatementKind.OTHER, null);

    private DatabendSqlClassifier() {
    }

    public static Classification classify(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return OTHER;
        }

        List<Token> tokens = tokenize(sql);
        int end = trimTrailingSemicolon(tokens);
        if (end <= 0 || hasNonTrailingSemicolon(tokens, end)) {
            return OTHER;
        }

        return classifyStatement(tokens, 0, end);
    }

    private static Classification classifyStatement(List<Token> tokens, int start, int end) {
        int index = start;
        if (matches(tokens, index, "SETTINGS")) {
            index++;
            if (index < end && isSymbol(tokens.get(index), "(")) {
                index = skipBalanced(tokens, index, end);
                if (index < 0) {
                    return OTHER;
                }
            }
            return classifyStatement(tokens, index, end);
        }

        if (matches(tokens, index, "WITH")) {
            int statementIndex = findTopLevelStatementAfterWith(tokens, index + 1, end);
            if (statementIndex < 0) {
                return OTHER;
            }
            return classifyStatement(tokens, statementIndex, end);
        }

        if (matches(tokens, index, "INSERT")) {
            return classifyInsert(tokens, index, end);
        }
        if (matches(tokens, index, "REPLACE")) {
            return classifyReplace(tokens, index, end);
        }
        return OTHER;
    }

    private static Classification classifyInsert(List<Token> tokens, int insertIndex, int end) {
        int index = insertIndex + 1;
        boolean overwrite = false;
        if (matches(tokens, index, "OVERWRITE")) {
            overwrite = true;
            index++;
        }
        if (matches(tokens, index, "FIRST") || matches(tokens, index, "ALL")) {
            return OTHER;
        }
        if (matches(tokens, index, "INTO")) {
            index++;
        } else if (!overwrite) {
            return OTHER;
        }
        if (matches(tokens, index, "TABLE")) {
            index++;
        }

        ParsedName table = parseQualifiedName(tokens, index, end);
        if (table == null) {
            return OTHER;
        }
        index = table.nextIndex;

        if (index < end && isSymbol(tokens.get(index), "(")) {
            index = skipBalanced(tokens, index, end);
            if (index < 0) {
                return OTHER;
            }
        }

        // Batched INSERT OVERWRITE is not equivalent to one staged execution:
        // JDBC batch semantics overwrite once per parameter set.
        return classifyInsertSource(tokens, index, end, table.name, StatementKind.INSERT_VALUES, !overwrite);
    }

    private static Classification classifyReplace(List<Token> tokens, int replaceIndex, int end) {
        int index = replaceIndex + 1;
        if (matches(tokens, index, "INTO")) {
            index++;
        }

        ParsedName table = parseQualifiedName(tokens, index, end);
        if (table == null) {
            return OTHER;
        }
        index = table.nextIndex;

        if (index < end && isSymbol(tokens.get(index), "(")) {
            index = skipBalanced(tokens, index, end);
            if (index < 0) {
                return OTHER;
            }
        }

        if (!matches(tokens, index, "ON")) {
            return OTHER;
        }
        index++;
        if (matches(tokens, index, "CONFLICT")) {
            index++;
        }
        if (index >= end || !isSymbol(tokens.get(index), "(")) {
            return OTHER;
        }
        index = skipBalanced(tokens, index, end);
        if (index < 0) {
            return OTHER;
        }
        if (matches(tokens, index, "DELETE")) {
            return new Classification(StatementKind.INSERT_SELECT_OR_LOAD, table.name);
        }

        return classifyInsertSource(tokens, index, end, table.name, StatementKind.REPLACE_VALUES, true);
    }

    private static Classification classifyInsertSource(
            List<Token> tokens,
            int sourceIndex,
            int end,
            String tableName,
            StatementKind valuesKind,
            boolean valuesSourceCanBatch) {
        if (sourceIndex >= end) {
            return OTHER;
        }
        if (matches(tokens, sourceIndex, "VALUES")) {
            if (!valuesSourceCanBatch || hasTopLevelFromAfter(tokens, sourceIndex + 1, end)) {
                return new Classification(StatementKind.INSERT_SELECT_OR_LOAD, tableName);
            }
            return new Classification(valuesKind, tableName);
        }
        return new Classification(StatementKind.INSERT_SELECT_OR_LOAD, tableName);
    }

    private static boolean hasTopLevelFromAfter(List<Token> tokens, int start, int end) {
        int depth = 0;
        for (int i = start; i < end; i++) {
            Token token = tokens.get(i);
            if (isSymbol(token, "(")) {
                depth++;
            } else if (isSymbol(token, ")")) {
                depth--;
                if (depth < 0) {
                    return false;
                }
            } else if (depth == 0 && matches(tokens, i, "FROM")) {
                return true;
            }
        }
        return false;
    }

    private static int findTopLevelStatementAfterWith(List<Token> tokens, int start, int end) {
        int depth = 0;
        for (int i = start; i < end; i++) {
            Token token = tokens.get(i);
            if (isSymbol(token, "(")) {
                depth++;
            } else if (isSymbol(token, ")")) {
                depth--;
                if (depth < 0) {
                    return -1;
                }
            } else if (depth == 0 && token.kind == TokenKind.WORD) {
                if (token.matches("INSERT") || token.matches("REPLACE")) {
                    return i;
                }
                if (token.matches("SELECT")) {
                    return -1;
                }
            }
        }
        return -1;
    }

    private static ParsedName parseQualifiedName(List<Token> tokens, int start, int end) {
        StringBuilder name = new StringBuilder();
        int index = start;
        boolean expectIdentifier = true;
        boolean foundIdentifier = false;

        while (index < end) {
            Token token = tokens.get(index);
            if (expectIdentifier) {
                if (!isIdentifierToken(token)) {
                    break;
                }
                if (foundIdentifier) {
                    name.append('.');
                }
                name.append(token.normalizedIdentifier());
                foundIdentifier = true;
                expectIdentifier = false;
                index++;
            } else if (isSymbol(token, ".")) {
                expectIdentifier = true;
                index++;
            } else {
                break;
            }
        }

        if (!foundIdentifier || expectIdentifier) {
            return null;
        }
        return new ParsedName(name.toString(), index);
    }

    private static boolean isIdentifierToken(Token token) {
        return token.kind == TokenKind.WORD || token.kind == TokenKind.QUOTED_IDENTIFIER;
    }

    private static int skipBalanced(List<Token> tokens, int openIndex, int end) {
        int depth = 0;
        for (int i = openIndex; i < end; i++) {
            Token token = tokens.get(i);
            if (isSymbol(token, "(")) {
                depth++;
            } else if (isSymbol(token, ")")) {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
                if (depth < 0) {
                    return -1;
                }
            }
        }
        return -1;
    }

    private static int trimTrailingSemicolon(List<Token> tokens) {
        int end = tokens.size();
        if (end > 0 && isSymbol(tokens.get(end - 1), ";")) {
            end--;
        }
        return end;
    }

    private static boolean hasNonTrailingSemicolon(List<Token> tokens, int end) {
        for (int i = 0; i < end; i++) {
            if (isSymbol(tokens.get(i), ";")) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(List<Token> tokens, int index, String keyword) {
        return index >= 0 && index < tokens.size() && tokens.get(index).matches(keyword);
    }

    private static boolean isSymbol(Token token, String symbol) {
        return token.kind == TokenKind.SYMBOL && token.text.equals(symbol);
    }

    private static List<Token> tokenize(String sql) {
        List<Token> tokens = new ArrayList<>();
        int index = 0;
        while (index < sql.length()) {
            char ch = sql.charAt(index);
            if (Character.isWhitespace(ch)) {
                index++;
            } else if (ch == '-' && hasNext(sql, index, '-')) {
                index = skipLineComment(sql, index + 2);
            } else if (ch == '/' && hasNext(sql, index, '*')) {
                index = skipBlockComment(sql, index + 2);
            } else if (ch == '\'') {
                index = readQuoted(sql, index, '\'', TokenKind.STRING, tokens);
            } else if (isIdentifierQuote(ch)) {
                index = readQuoted(sql, index, ch, TokenKind.QUOTED_IDENTIFIER, tokens);
            } else if (isWordStart(ch)) {
                int start = index;
                index++;
                while (index < sql.length() && isWordPart(sql.charAt(index))) {
                    index++;
                }
                tokens.add(new Token(sql.substring(start, index), TokenKind.WORD));
            } else {
                tokens.add(new Token(String.valueOf(ch), TokenKind.SYMBOL));
                index++;
            }
        }
        return tokens;
    }

    private static int readQuoted(
            String sql,
            int start,
            char quote,
            TokenKind kind,
            List<Token> tokens) {
        StringBuilder text = new StringBuilder();
        int index = start + 1;
        while (index < sql.length()) {
            char ch = sql.charAt(index);
            if (ch == quote) {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                    text.append(quote);
                    index += 2;
                } else {
                    index++;
                    break;
                }
            } else if (ch == '\\' && quote == '\'' && index + 1 < sql.length()) {
                text.append(sql.charAt(index + 1));
                index += 2;
            } else {
                text.append(ch);
                index++;
            }
        }
        tokens.add(new Token(text.toString(), kind));
        return index;
    }

    private static int skipLineComment(String sql, int index) {
        while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
            index++;
        }
        return index;
    }

    private static int skipBlockComment(String sql, int index) {
        while (index + 1 < sql.length()) {
            if (sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
                return index + 2;
            }
            index++;
        }
        return sql.length();
    }

    private static boolean hasNext(String sql, int index, char expected) {
        return index + 1 < sql.length() && sql.charAt(index + 1) == expected;
    }

    private static boolean isWordStart(char ch) {
        return Character.isLetter(ch) || ch == '_' || ch == '$';
    }

    private static boolean isWordPart(char ch) {
        return Character.isLetterOrDigit(ch) || ch == '_' || ch == '$' || ch == '-';
    }

    private static boolean isIdentifierQuote(char ch) {
        // Databend dialects accept backticks as identifier quotes; PostgreSQL-like
        // dialects also accept double quotes.
        return ch == '`' || ch == '"';
    }

    private static final class ParsedName {
        private final String name;
        private final int nextIndex;

        private ParsedName(String name, int nextIndex) {
            this.name = name;
            this.nextIndex = nextIndex;
        }
    }

    private static final class Token {
        private final String text;
        private final TokenKind kind;

        private Token(String text, TokenKind kind) {
            this.text = text;
            this.kind = kind;
        }

        private boolean matches(String keyword) {
            return kind == TokenKind.WORD && text.equalsIgnoreCase(keyword);
        }

        private String normalizedIdentifier() {
            return text;
        }
    }
}
