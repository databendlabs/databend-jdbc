package com.databend.client.data;

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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateHandler implements ColumnTypeHandler {
    private final boolean isNullable;
    private final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;

    public DateHandler() {
        this.isNullable = false;
    }

    public DateHandler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (value == null) {
            if (isNullable) {
                return null;
            }
            throw new IllegalArgumentException("Date type is not nullable");
        }

        if (value instanceof LocalDate) {
            return value;
        }

        if (value instanceof String) {
            try {
                return LocalDate.parse((String) value, formatter);
            }
            catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid date format: " + value, e);
            }
        }

        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Date");
    }

    @Override
    public void setNullable(boolean isNullable) {
        // do nothing
    }
}
