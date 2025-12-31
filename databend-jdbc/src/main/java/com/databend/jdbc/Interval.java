package com.databend.jdbc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

    /**
     * Utility to decode interval strings emitted by Databend into {@link Duration}, and encode {@link Duration}
     * into the microsecond literal format accepted by Databend.
     */
public final class Interval {
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MINUTE = 60L * MICROS_PER_SECOND;
    private static final long MICROS_PER_HOUR = 60L * MICROS_PER_MINUTE;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;
    private static final long MONTHS_PER_YEAR = 12;

    private Interval() {}

    public static Duration decode(String value) {
        Objects.requireNonNull(value, "interval string is null");
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("interval string is empty");
        }

        List<String> tokens = tokenize(trimmed);
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("interval string is empty");
        }

        long totalMonths = 0L;
        long totalDays = 0L;
        long timeMicros = 0L;
        boolean timeParsed = false;

        for (int i = 0; i < tokens.size();) {
            String token = tokens.get(i);
            if (isTimeToken(token)) {
                if (timeParsed) {
                    throw new IllegalArgumentException("duplicate time section in interval: " + value);
                }
                timeMicros = parseTimeMicros(token);
                timeParsed = true;
                i++;
                continue;
            }

            long numeric = parseLong(token, value);
            i++;
            if (i >= tokens.size()) {
                throw new IllegalArgumentException("missing unit after numeric value in interval: " + value);
            }
            String unitToken = tokens.get(i);
            i++;
            String normalized = normalizeUnit(unitToken);
            switch (normalized) {
                case "year":
                    totalMonths = Math.addExact(totalMonths, Math.multiplyExact(numeric, MONTHS_PER_YEAR));
                    break;
                case "month":
                    totalMonths = Math.addExact(totalMonths, numeric);
                    break;
                case "day":
                    totalDays = Math.addExact(totalDays, numeric);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "unsupported interval unit '" + unitToken + "' in: " + value);
            }
        }

        if (totalMonths != 0L) {
            throw new IllegalArgumentException(
                    "year/month components cannot be represented as Duration: " + value);
        }

        long dayMicros = Math.multiplyExact(totalDays, MICROS_PER_DAY);
        long totalMicros = Math.addExact(dayMicros, timeMicros);

        long seconds = Math.floorDiv(totalMicros, MICROS_PER_SECOND);
        long microsRemainder = Math.floorMod(totalMicros, MICROS_PER_SECOND);
        long nanoAdjustment = Math.multiplyExact(microsRemainder, 1_000L);
        return Duration.ofSeconds(seconds, nanoAdjustment);
    }

    public static String encode(Duration duration) {
        Objects.requireNonNull(duration, "duration is null");
        int nanos = duration.getNano();
        if (nanos % 1_000 != 0) {
            throw new IllegalArgumentException("Duration precision finer than microseconds: " + duration);
        }

        long microsFromSeconds;
        try {
            microsFromSeconds = Math.multiplyExact(duration.getSeconds(), 1000000L);
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException("Duration too large to encode: " + duration, ex);
        }

        long microsFromNanos = nanos / 1_000L;
        long totalMicros;
        try {
            totalMicros = Math.addExact(microsFromSeconds, microsFromNanos);
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException("Duration too large to encode: " + duration, ex);
        }

        return Long.toString(totalMicros);
    }

    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        int length = input.length();
        int start = 0;
        while (start < length) {
            while (start < length && Character.isWhitespace(input.charAt(start))) {
                start++;
            }
            if (start >= length) {
                break;
            }
            int end = start;
            while (end < length && !Character.isWhitespace(input.charAt(end))) {
                end++;
            }
            tokens.add(input.substring(start, end));
            start = end;
        }
        return tokens;
    }

    private static boolean isTimeToken(String token) {
        int colonCount = 0;
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (i == 0 && (c == '-' || c == '+')) {
                continue;
            }
            if (c == ':') {
                colonCount++;
            } else if (!Character.isDigit(c) && c != '.') {
                return false;
            }
        }
        return colonCount == 2;
    }

    private static long parseTimeMicros(String token) {
        boolean negative = token.startsWith("-");
        boolean positive = token.startsWith("+");
        if (negative || positive) {
            token = token.substring(1);
        }
        String[] parts = token.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("invalid time component: " + token);
        }

        long hours = parseNonNegativeLong(parts[0], "hours");
        long minutes = parseNonNegativeLong(parts[1], "minutes");
        String secondsSection = parts[2];
        long seconds;
        long microsFraction = 0L;
        int dotIndex = secondsSection.indexOf('.');
        if (dotIndex >= 0) {
            String secondsStr = secondsSection.substring(0, dotIndex);
            String fraction = secondsSection.substring(dotIndex + 1);
            seconds = parseNonNegativeLong(secondsStr.isEmpty() ? "0" : secondsStr, "seconds");
            microsFraction = parseFractionMicros(fraction);
        } else {
            seconds = parseNonNegativeLong(secondsSection, "seconds");
        }

        validateMinuteSecondRange(minutes, seconds);
        long micros = Math.addExact(Math.addExact(Math.multiplyExact(hours, MICROS_PER_HOUR),
                Math.multiplyExact(minutes, MICROS_PER_MINUTE)),
                Math.addExact(Math.multiplyExact(seconds, MICROS_PER_SECOND), microsFraction));
        return negative ? -micros : micros;
    }

    private static void validateMinuteSecondRange(long minutes, long seconds) {
        if (minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60) {
            throw new IllegalArgumentException("minutes/seconds out of range in interval time section");
        }
    }

    private static long parseFractionMicros(String fraction) {
        if (fraction.isEmpty()) {
            return 0L;
        }
        if (fraction.length() > 6) {
            throw new IllegalArgumentException("fractional seconds precision exceeds microseconds: " + fraction);
        }
        long value = parseNonNegativeLong(fraction, "fraction");
        for (int i = fraction.length(); i < 6; i++) {
            value *= 10;
        }
        return value;
    }

    private static long parseNonNegativeLong(String number, String field) {
        if (number.isEmpty()) {
            throw new IllegalArgumentException("empty " + field + " token in interval");
        }
        long parsed = Long.parseLong(number);
        if (parsed < 0) {
            throw new IllegalArgumentException(field + " must be non-negative in interval segment");
        }
        return parsed;
    }

    private static long parseLong(String token, String original) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid numeric value '" + token + "' in interval: " + original, ex);
        }
    }

    private static String normalizeUnit(String unit) {
        String normalized = unit.toLowerCase(Locale.ROOT);
        if (normalized.endsWith("s")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

}
