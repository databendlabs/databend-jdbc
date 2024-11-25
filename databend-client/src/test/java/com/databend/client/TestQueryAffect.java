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

import com.databend.client.utils.JsonCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.databend.client.utils.JsonCodec.jsonCodec;


@Test(timeOut = 10000)
public class TestQueryAffect {
    private static final JsonCodec<QueryAffect> QUERY_AFFECT_JSON_CODEC = jsonCodec(QueryAffect.class);

    @Test( groups = {"unit"} )
    public void testQueryAffectUseDB() throws JsonProcessingException {
        String json = "{\"type\":\"UseDB\",\"name\":\"db1\"}";
        QueryAffect clause = QUERY_AFFECT_JSON_CODEC.fromJson(json);

        // find the right static class
        Assert.assertEquals(clause.getClass(), QueryAffect.UseDB.class);
        QueryAffect.UseDB useDB = (QueryAffect.UseDB) clause;
        Assert.assertEquals(useDB.getName(), "db1");
    }

    @Test( groups = {"unit"} )
    public void testQueryAffectChangeSettings() throws JsonProcessingException {
        String json = "{\"type\":\"ChangeSettings\",\"keys\":[\"max_threads\"],\"values\":[\"1\"],\"is_globals\":[false]}";
        QueryAffect clause = QUERY_AFFECT_JSON_CODEC.fromJson(json);

        // find the right static class
        Assert.assertEquals(clause.getClass(), QueryAffect.ChangeSettings.class);
        QueryAffect.ChangeSettings changeSettings = (QueryAffect.ChangeSettings) clause;
        Assert.assertEquals(changeSettings.getIsGlobals().size(), 1);
        Assert.assertEquals(changeSettings.getIsGlobals().get(0).booleanValue(), false);
        Assert.assertEquals(changeSettings.getKeys().size(), 1);
        Assert.assertEquals(changeSettings.getKeys().get(0), "max_threads");
        Assert.assertEquals(changeSettings.getValues().size(), 1);
    }

}
