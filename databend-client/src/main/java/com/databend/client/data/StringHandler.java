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

class StringHandler implements ColumnTypeHandler {
    private final boolean isNullable;

    public StringHandler() {
        this.isNullable = false;
    }

    public StringHandler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (value == null) {
            if (isNullable) {
                return null;
            } else {
                throw new IllegalArgumentException("String type is not nullable");
            }
        }
        if (value instanceof String) {
            return value;
        }
        return value.toString();
    }

    @Override
    public void setNullable(boolean isNullable) {
        // do nothing
    }
}
