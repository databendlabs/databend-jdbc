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

package com.databend.client;

import com.databend.client.errors.QueryErrors;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.databend.client.JsonCodec.jsonCodec;



@Test(timeOut = 10000)
public class TestQueryErrors
{
    private static final JsonCodec<QueryErrors> QUERY_ERROR_JSON_CODEC = jsonCodec(QueryErrors.class);

    @Test( groups = {"unit"} )
    public void testQueryError() throws JsonProcessingException {
        String json = "{\"code\": 1000, \"message\": \"test\"}";
        Assert.assertEquals(QUERY_ERROR_JSON_CODEC.fromJson(json).getCode(), 1000);
        Assert.assertEquals(QUERY_ERROR_JSON_CODEC.fromJson(json).getMessage(), "test");
    }
}
