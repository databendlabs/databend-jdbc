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

public abstract class ColumnTypeHandlerBase implements ColumnTypeHandler {
    protected boolean isNullable;

    public ColumnTypeHandlerBase(boolean isNullable) {
        this.isNullable = isNullable;
    }
    protected boolean isNull(String value) {
        return (value == null || "NULL".equals(value));
    }

    private boolean checkNull(String value){
        if (isNull(value)) {
            if (isNullable) {
                return true;
            } else {
                throw new IllegalArgumentException("type " + this.getClass().getName() + " is not nullable, but got " + value);
            }
        }
        return false;
    }

    @Override
    public Object parseString(String value) {
        if (checkNull(value)) {
            return null;
        }
        return parseStringNotNull(value);
    }
    @Override
    public Object parseValue(Object value) {
        return parseString((String) value);
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    abstract Object parseStringNotNull(String value);
}
