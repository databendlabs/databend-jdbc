package com.databend.jdbc;

import java.time.Duration;

public final class IntervalValue {
    private final int days;
    private final long micros;

    public IntervalValue(int days, long micros) {
        this.days = days;
        this.micros = micros;
    }

    public Duration asDuration() {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Duration.ofDays(days).plusSeconds(seconds).plusNanos(nanos);
    }

    public String asString() {
        StringBuilder builder = new StringBuilder();
        if (days != 0) {
            builder.append(days).append(" day");
            if (Math.abs(days) != 1) {
                builder.append('s');
            }
            if (micros != 0) {
                builder.append(' ');
            }
        }
        if (micros != 0 || days == 0) {
            boolean negative = micros < 0;
            long absMicros = Math.abs(micros);
            long hours = absMicros / (60L * 60L * 1_000_000L);
            absMicros %= 60L * 60L * 1_000_000L;
            long minutes = absMicros / (60L * 1_000_000L);
            absMicros %= 60L * 1_000_000L;
            long seconds = absMicros / 1_000_000L;
            long fractionalMicros = absMicros % 1_000_000L;
            if (negative) {
                builder.append('-');
            }
            if (days == 0 && micros == 0 && hours < 10) {
                builder.append('0');
            }
            builder.append(hours).append(':');
            if (minutes < 10) {
                builder.append('0');
            }
            builder.append(minutes).append(':');
            if (seconds < 10) {
                builder.append('0');
            }
            builder.append(seconds);
            if (fractionalMicros != 0) {
                String fraction = String.format("%06d", fractionalMicros).replaceFirst("0+$", "");
                builder.append('.').append(fraction);
            }
        }
        return builder.toString();
    }
}
