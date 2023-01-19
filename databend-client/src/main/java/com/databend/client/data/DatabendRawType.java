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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;

// it could be either a string or a struct if it is not nullable
public class DatabendRawType {
    private final String type;

    @JsonCreator
    public DatabendRawType(
            String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }


    public boolean isNullable() {
        return type.contains(DatabendTypes.NULLABLE) || type.contains(DatabendTypes.NULL);
    }

    public static boolean isArrayType(String type) {
        Pattern pattern = Pattern.compile("(\\w+)\\((\\w+)\\)");
        Matcher matcher = pattern.matcher(type);

        if (matcher.find()) {
            String arrayType = matcher.group(1);
            String innerType = matcher.group(2);
        }
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("type", type).toString();
    }
}
