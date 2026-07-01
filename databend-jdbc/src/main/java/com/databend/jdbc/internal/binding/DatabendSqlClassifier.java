package com.databend.jdbc.internal.binding;

import java.util.Optional;

public final class DatabendSqlClassifier {
    public enum StatementKind {
        INSERT_VALUES,
        REPLACE_VALUES,
        OTHER
    }

    public static final class Classification {
        private final StatementKind kind;

        private Classification(StatementKind kind) {
            this.kind = kind;
        }

        public StatementKind getKind() {
            return kind;
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

    private static final Classification INSERT_VALUES = new Classification(StatementKind.INSERT_VALUES);
    private static final Classification REPLACE_VALUES = new Classification(StatementKind.REPLACE_VALUES);
    private static final Classification OTHER = new Classification(StatementKind.OTHER);

    private DatabendSqlClassifier() {
    }

    public static Classification classify(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return OTHER;
        }

        TokenCursor cursor = new TokenCursor(sql);
        Token first = cursor.next();
        if (first == null) {
            return OTHER;
        }
        return classifyStatement(first, cursor);
    }

    public static boolean isQuery(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }

        TokenCursor cursor = new TokenCursor(sql);
        Token first = cursor.nextStatementToken();
        return first != null && isQueryStatement(first, cursor);
    }

    public static Optional<Integer> countInsertTargetColumns(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return Optional.empty();
        }

        TokenCursor cursor = new TokenCursor(sql);
        Token first = cursor.next();
        if (first == null) {
            return Optional.empty();
        }
        return countInsertTargetColumns(first, cursor);
    }

    private static Classification classifyStatement(Token first, TokenCursor cursor) {
        if (first.matches("SETTINGS")) {
            Token next = cursor.next();
            if (next != null && next.isSymbol("(")) {
                if (!cursor.skipBalancedAfterOpen()) {
                    return OTHER;
                }
                next = cursor.next();
            }
            return next == null ? OTHER : classifyStatement(next, cursor);
        }

        if (first.matches("WITH")) {
            Token statementToken = cursor.findTopLevelStatementAfterWith();
            return statementToken == null ? OTHER : classifyStatement(statementToken, cursor);
        }

        if (first.matches("INSERT")) {
            return classifyInsert(cursor);
        }
        if (first.matches("REPLACE")) {
            return classifyReplace(cursor);
        }
        return OTHER;
    }

    private static Optional<Integer> countInsertTargetColumns(Token first, TokenCursor cursor) {
        if (first.matches("SETTINGS")) {
            Token next = cursor.next();
            if (next != null && next.isSymbol("(")) {
                if (!cursor.skipBalancedAfterOpen()) {
                    return Optional.empty();
                }
                next = cursor.next();
            }
            return next == null ? Optional.<Integer>empty() : countInsertTargetColumns(next, cursor);
        }

        if (first.matches("WITH")) {
            Token statementToken = cursor.findTopLevelStatementAfterWith();
            return statementToken == null ? Optional.<Integer>empty() : countInsertTargetColumns(statementToken, cursor);
        }

        if (first.matches("INSERT")) {
            return countInsertColumns(cursor);
        }
        if (first.matches("REPLACE")) {
            return countReplaceColumns(cursor);
        }
        return Optional.empty();
    }

    private static boolean isQueryStatement(Token first, TokenCursor cursor) {
        if (first.matches("SETTINGS")) {
            Token next = cursor.next();
            if (next != null && next.isSymbol("(")) {
                if (!cursor.skipBalancedAfterOpen()) {
                    return false;
                }
                next = cursor.nextStatementToken();
            }
            return next != null && isQueryStatement(next, cursor);
        }

        if (first.matches("WITH")) {
            Token statementToken = cursor.findTopLevelStatementAfterWith();
            return statementToken != null && isQueryStatement(statementToken, cursor);
        }

        return first.matches("SHOW")
                || first.matches("SELECT")
                || first.matches("DESCRIBE")
                || first.matches("EXISTS")
                || first.matches("EXPLAIN")
                || first.matches("CALL");
    }

    private static Optional<Integer> countInsertColumns(TokenCursor cursor) {
        Token token = cursor.next();
        if (token != null && token.matches("OVERWRITE")) {
            token = cursor.next();
        }
        if (token == null || !token.matches("INTO")) {
            return Optional.empty();
        }

        token = cursor.next();
        if (token != null && token.matches("TABLE")) {
            token = cursor.next();
        }
        if (!cursor.skipQualifiedName(token)) {
            return Optional.empty();
        }

        token = cursor.next();
        if (token == null || !token.isSymbol("(")) {
            return Optional.empty();
        }
        return cursor.countCommaSeparatedItemsAfterOpen();
    }

    private static Optional<Integer> countReplaceColumns(TokenCursor cursor) {
        Token token = cursor.next();
        if (token != null && token.matches("INTO")) {
            token = cursor.next();
        }
        if (!cursor.skipQualifiedName(token)) {
            return Optional.empty();
        }

        token = cursor.next();
        if (token == null || !token.isSymbol("(")) {
            return Optional.empty();
        }
        return cursor.countCommaSeparatedItemsAfterOpen();
    }

    private static Classification classifyInsert(TokenCursor cursor) {
        Token token = cursor.next();
        if (token == null) {
            return OTHER;
        }
        if (token.matches("OVERWRITE")) {
            // Batched INSERT OVERWRITE is not equivalent to one staged execution:
            // JDBC batch semantics overwrite once per parameter set.
            return OTHER;
        }
        if (!token.matches("INTO")) {
            return OTHER;
        }

        token = cursor.next();
        if (token != null && token.matches("TABLE")) {
            token = cursor.next();
        }

        if (!cursor.skipQualifiedName(token)) {
            return OTHER;
        }

        token = cursor.next();
        if (token != null && token.isSymbol("(")) {
            if (!cursor.skipBalancedAfterOpen()) {
                return OTHER;
            }
            token = cursor.next();
        }

        if (token == null || !token.matches("VALUES")) {
            return OTHER;
        }
        return cursor.valuesRemainderIsBatchSafe() ? INSERT_VALUES : OTHER;
    }

    private static Classification classifyReplace(TokenCursor cursor) {
        Token token = cursor.next();
        if (token != null && token.matches("INTO")) {
            token = cursor.next();
        }

        if (!cursor.skipQualifiedName(token)) {
            return OTHER;
        }

        token = cursor.next();
        if (token != null && token.isSymbol("(")) {
            if (!cursor.skipBalancedAfterOpen()) {
                return OTHER;
            }
            token = cursor.next();
        }

        if (token == null || !token.matches("ON")) {
            return OTHER;
        }
        token = cursor.next();
        if (token != null && token.matches("CONFLICT")) {
            token = cursor.next();
        }
        if (token == null || !token.isSymbol("(")) {
            return OTHER;
        }
        if (!cursor.skipBalancedAfterOpen()) {
            return OTHER;
        }

        token = cursor.next();
        if (token == null || token.matches("DELETE") || !token.matches("VALUES")) {
            return OTHER;
        }
        return cursor.valuesRemainderIsBatchSafe() ? REPLACE_VALUES : OTHER;
    }

    private static boolean isIdentifierToken(Token token) {
        return token != null && (token.kind == TokenKind.WORD || token.kind == TokenKind.QUOTED_IDENTIFIER);
    }

    private static boolean isIdentifierQuote(char ch) {
        // Databend dialects accept backticks as identifier quotes; PostgreSQL-like
        // dialects also accept double quotes.
        return ch == '`' || ch == '"';
    }

    private static boolean isWordStart(char ch) {
        return Character.isLetter(ch) || ch == '_' || ch == '$';
    }

    private static boolean isWordPart(char ch) {
        return Character.isLetterOrDigit(ch) || ch == '_' || ch == '$';
    }

    private static final class TokenCursor {
        private final String sql;
        private int index;
        private Token peeked;

        private TokenCursor(String sql) {
            this.sql = sql;
        }

        private Token peek() {
            if (peeked == null) {
                peeked = readNext();
            }
            return peeked;
        }

        private Token next() {
            Token token = peek();
            peeked = null;
            return token;
        }

        private Token nextStatementToken() {
            Token token;
            do {
                token = next();
            } while (token != null && token.isSymbol("("));
            return token;
        }

        private boolean skipBalancedAfterOpen() {
            int depth = 1;
            Token token;
            while ((token = next()) != null) {
                if (token.isSymbol("(")) {
                    depth++;
                } else if (token.isSymbol(")")) {
                    depth--;
                    if (depth == 0) {
                        return true;
                    }
                }
            }
            return false;
        }

        private Optional<Integer> countCommaSeparatedItemsAfterOpen() {
            int depth = 1;
            int count = 0;
            boolean hasItem = false;
            Token token;
            while ((token = next()) != null) {
                if (token.isSymbol("(")) {
                    depth++;
                    hasItem = true;
                } else if (token.isSymbol(")")) {
                    depth--;
                    if (depth < 0) {
                        return Optional.empty();
                    }
                    if (depth == 0) {
                        return Optional.of(hasItem ? count + 1 : 0);
                    }
                    hasItem = true;
                } else if (depth == 1 && token.isSymbol(",")) {
                    if (!hasItem) {
                        return Optional.empty();
                    }
                    count++;
                    hasItem = false;
                } else {
                    hasItem = true;
                }
            }
            return Optional.empty();
        }

        private Token findTopLevelStatementAfterWith() {
            int depth = 0;
            Token token;
            while ((token = next()) != null) {
                if (token.isSymbol("(")) {
                    depth++;
                } else if (token.isSymbol(")")) {
                    depth--;
                    if (depth < 0) {
                        return null;
                    }
                } else if (depth == 0 && token.kind == TokenKind.WORD) {
                    if (isStatementStartAfterWith(token)) {
                        return token;
                    }
                }
            }
            return null;
        }

        private boolean skipQualifiedName(Token first) {
            if (!isIdentifierToken(first)) {
                return false;
            }

            while (true) {
                Token dot = peek();
                if (dot == null || !dot.isSymbol(".")) {
                    return true;
                }
                next();
                Token part = next();
                if (!isIdentifierToken(part)) {
                    return false;
                }
            }
        }

        private boolean valuesRemainderIsBatchSafe() {
            int depth = 0;
            Token token;
            while ((token = next()) != null) {
                if (token.isSymbol("(")) {
                    depth++;
                } else if (token.isSymbol(")")) {
                    depth--;
                    if (depth < 0) {
                        return false;
                    }
                } else if (depth == 0 && token.matches("FROM")) {
                    return false;
                } else if (token.isSymbol(";")) {
                    return depth == 0 && onlyEndRemains();
                }
            }
            return true;
        }

        private boolean onlyEndRemains() {
            return next() == null;
        }

        private Token readNext() {
            while (index < sql.length()) {
                char ch = sql.charAt(index);
                if (Character.isWhitespace(ch)) {
                    index++;
                } else if (ch == '-' && hasNext('-')) {
                    skipLineComment();
                } else if (ch == '/' && hasNext('*')) {
                    skipBlockComment();
                } else if (ch == '\'') {
                    return readQuoted('\'', TokenKind.STRING);
                } else if (isIdentifierQuote(ch)) {
                    return readQuoted(ch, TokenKind.QUOTED_IDENTIFIER);
                } else if (isWordStart(ch)) {
                    return readWord();
                } else {
                    index++;
                    return new Token(String.valueOf(ch), TokenKind.SYMBOL);
                }
            }
            return null;
        }

        private Token readWord() {
            int start = index;
            index++;
            while (index < sql.length() && isWordPart(sql.charAt(index))) {
                index++;
            }
            return new Token(sql.substring(start, index), TokenKind.WORD);
        }

        private Token readQuoted(char quote, TokenKind kind) {
            StringBuilder text = new StringBuilder();
            index++;
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
            return new Token(text.toString(), kind);
        }

        private void skipLineComment() {
            index += 2;
            while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
                index++;
            }
        }

        private void skipBlockComment() {
            index += 2;
            while (index + 1 < sql.length()) {
                if (sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
                    index += 2;
                    return;
                }
                index++;
            }
            index = sql.length();
        }

        private boolean hasNext(char expected) {
            return index + 1 < sql.length() && sql.charAt(index + 1) == expected;
        }
    }

    private static boolean isStatementStartAfterWith(Token token) {
        return token.matches("INSERT")
                || token.matches("REPLACE")
                || token.matches("SHOW")
                || token.matches("SELECT")
                || token.matches("DESCRIBE")
                || token.matches("EXISTS")
                || token.matches("EXPLAIN")
                || token.matches("CALL");
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

        private boolean isSymbol(String symbol) {
            return kind == TokenKind.SYMBOL && text.equals(symbol);
        }

    }
}
