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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class ArrayHandler implements ColumnTypeHandler {

    private final ColumnTypeHandler elementTypeHandler;
    private boolean isNullable;

    public ArrayHandler(DatabendRawType type) {
        requireNonNull(type, "type is null");
        checkArgument(type.getType().equals(DatabendTypes.ARRAY), "type must be array type");
        requireNonNull(type.getInner(), "array inner type is null");
        this.elementTypeHandler = ColumnTypeHandlerFactory.getTypeHandler(type.getInner());
        this.isNullable = false;
    }

    @Override
    public Object parseValue(Object value) {
        List<?> listValue = (List<?>) value;
        ArrayList<Object> result = new ArrayList<>(listValue.size());
        for (Object item : listValue) {
            if (item != null) {
                item = elementTypeHandler.parseValue(item);
            }
            result.add(item);
        }
        return result;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}
