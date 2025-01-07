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

public interface ColumnTypeHandler {
    /**
     * Convert the input object to the corresponding Java Type
     *
     * @param value raw input row value
     * @return parsed java type value
     */
    Object parseValue(String value) throws IllegalArgumentException;

    void setNullable(boolean isNullable);
}
