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

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

// it could be either a string or a struct if it is not nullable
public class DatabendRawType {

    private final String type;
    private final boolean isNullable;
    private final DatabendDataType dataType;

    private Integer columnSize = null;
    private Integer decimalDigits = null;
    private List<DatabendRawType> subType = null;

    @JsonCreator
    public DatabendRawType(String type) {
        // pattern match on Nullable(String) ignore case, and extract the inner type
        // The inner type does not handle "()" again to prevent the type words missing
        if (startsWithIgnoreCase(type, "Nullable")) {
            this.isNullable = true;
            this.type = type.substring(9, type.length() - 1);
        } else {
            this.isNullable = false;
            this.type = type;
        }
        // Get data type
        this.dataType = DatabendDataType.getByTypeName(this.type);
        if (dataType == DatabendDataType.DECIMAL) {
            // Numeric with decimals
            if (this.type.contains(",")) {
                this.columnSize = Integer.valueOf(this.type.substring((this.type.indexOf("(") + 1), (this.type.indexOf(","))).trim());
                this.decimalDigits = Integer.valueOf(this.type.substring((this.type.indexOf(",") + 1), (this.type.indexOf(")"))).trim());
            } else {
                this.columnSize = dataType.getLength();
                this.decimalDigits = 0;
            }
        } else if (dataType == DatabendDataType.ARRAY) {
            // remove "Array(" and last ")"
            String subTypeName = this.type.substring(6, this.type.length() - 1);
            this.subType = Collections.singletonList(new DatabendRawType(subTypeName));
        } else if (dataType == DatabendDataType.TUPLE) {
            // remove "Tuple(" and last ")"
            String subTypes = this.type.substring(6, this.type.length() - 1);
            // split by ","
            this.subType = splitByComma(subTypes).stream()
                    .map(DatabendRawType::new)
                    .collect(Collectors.toList());
            this.columnSize = subType.size();
        } else if (dataType == DatabendDataType.MAP) {
            // remove "Map(" and last ")"
            String subTypes = this.type.substring(4, this.type.length() - 1);
            // split by ","
            this.subType = splitByComma(subTypes).stream()
                    .map(DatabendRawType::new)
                    .collect(Collectors.toList());
            this.columnSize = subType.size();
        }
        if (this.columnSize == null) {
            this.columnSize = this.dataType.getLength();
        }
    }

    /**
     * string start with ignore case
     */
    public static boolean startsWithIgnoreCase(String str, String prefix) {
        if (str == null || prefix == null) {
            return false;
        }
        return str.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * Split using commas, only the topmost types are processed
     */
    private List<String> splitByComma(String types) {
        List<String> splitTypes = new ArrayList<>();
        StringBuilder splitType = new StringBuilder();
        int commaTotal = 0;
        for (int i = 0, size = types.length(); i < size; i++) {
            char word = types.charAt(i);
            if ('(' == word) {
                commaTotal++;
            } else if (')' == word) {
                commaTotal--;
            }
            if (',' == word && commaTotal == 0) {
                splitTypes.add(splitType.toString().trim());
                splitType = new StringBuilder();
            } else {
                splitType.append(word);
            }
        }
        if (splitType.length() > 0) {
            splitTypes.add(splitType.toString());
        }
        return splitTypes;
    }

    public String getType() {
        return type;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public DatabendDataType getDataType() {
        return dataType;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public Integer getDecimalDigits() {
        return decimalDigits;
    }

    public List<DatabendRawType> getSubType() {
        return subType;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("type", type)
                .add("subType", subType)
                .add("isNullable", isNullable)
                .add("dataType", dataType)
                .add("columnSize", columnSize)
                .add("decimalDigits", decimalDigits)
                .toString();
    }
}
