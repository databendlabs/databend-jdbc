/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databend.client.data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;

class TimestampHandler implements ColumnTypeHandler {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter();

    private static final List<DateTimeFormatter> FORMATTERS = Arrays.asList(
            TIMESTAMP_FORMATTER,
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    );

    private final ZoneId zoneId;
    private boolean isNullable;

    public TimestampHandler() {
        this(false);
    }

    public TimestampHandler(boolean isNullable) {
        this(isNullable, ZoneId.systemDefault());
    }

    public TimestampHandler(boolean isNullable, ZoneId zoneId) {
        this.isNullable = isNullable;
        this.zoneId = zoneId;
    }

    @Override
    public Object parseValue(Object value) {
        if (value == null) {
            if (isNullable) {
                return null;
            }
            throw new IllegalArgumentException("Timestamp type is not nullable");
        }

        if (value instanceof Instant) {
            return value;
        }

        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(zoneId).toInstant();
        }

        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toInstant();
        }

        if (value instanceof String) {
            String timestampStr = ((String) value).trim();

            try {
                long epochSeconds = Long.parseLong(timestampStr);
                return Instant.ofEpochSecond(epochSeconds);
            }
            catch (NumberFormatException ignored) {
            }

            try {
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, TIMESTAMP_FORMATTER);
                return localDateTime.atZone(zoneId).toInstant();
            }
            catch (DateTimeParseException e) {
                for (DateTimeFormatter formatter : FORMATTERS) {
                    try {
                        if (timestampStr.contains("T") && timestampStr.contains("Z")) {
                            return Instant.parse(timestampStr);
                        }
                        else {
                            LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                            return localDateTime.atZone(zoneId).toInstant();
                        }
                    }
                    catch (DateTimeParseException ignored) {
                    }
                }
            }

            throw new IllegalArgumentException("Unable to parse timestamp: " + timestampStr);
        }

        if (value instanceof Number) {
            return Instant.ofEpochMilli(((Number) value).longValue());
        }

        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Timestamp");
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}