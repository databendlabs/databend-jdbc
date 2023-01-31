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

import io.airlift.json.JsonCodec;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;

public class TestQueryResults {
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @Test(groups = {"unit"})
    public void testBasic() {
        String goldenValue = "{\"id\":\"5c4e776a-8171-462a-b2d3-6a34823d0552\",\"session_id\":\"3563624b-8767-44ff-a235-3f5bb4e54d03\",\"session\":{},\"schema\":[{\"name\":\"(number / 3)\",\"type\":\"Float64\"},{\"name\":\"(number + 1)\",\"type\":\"UInt64\"}],\"data\":[[\"0.0\",\"1\"],[\"0.3333333333333333\",\"2\"],[\"0.6666666666666666\",\"3\"],[\"1.0\",\"4\"],[\"1.3333333333333333\",\"5\"],[\"1.6666666666666667\",\"6\"],[\"2.0\",\"7\"],[\"2.3333333333333335\",\"8\"],[\"2.6666666666666665\",\"9\"],[\"3.0\",\"10\"]],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":10,\"bytes\":80},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":10,\"bytes\":160},\"running_time_ms\":1.494205},\"affect\":null,\"stats_uri\":\"/v1/query/5c4e776a-8171-462a-b2d3-6a34823d0552\",\"final_uri\":\"/v1/query/5c4e776a-8171-462a-b2d3-6a34823d0552/final\",\"next_uri\":\"/v1/query/5c4e776a-8171-462a-b2d3-6a34823d0552/final\",\"kill_uri\":\"/v1/query/5c4e776a-8171-462a-b2d3-6a34823d0552/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenValue);
        Assert.assertEquals(queryResults.getId(), "5c4e776a-8171-462a-b2d3-6a34823d0552");
        Assert.assertEquals(queryResults.getSessionId(), "3563624b-8767-44ff-a235-3f5bb4e54d03");
        Assert.assertEquals(queryResults.getSchema().size(), 2);
        Assert.assertEquals(queryResults.getSchema().get(0).getName(), "(number / 3)");
        Assert.assertEquals(queryResults.getSchema().get(0).getDataType().getType(), "Float64");
        Assert.assertEquals(queryResults.getSchema().get(1).getName(), "(number + 1)");
        Assert.assertEquals(queryResults.getSchema().get(1).getDataType().getType(), "UInt64");
        for (List<Object> row : queryResults.getData()) {
            Assert.assertEquals(row.size(), 2);
        }
        Assert.assertEquals(queryResults.getState(), "Succeeded");
    }

    @Test(groups = "unit")
    public void TestError() {
        String goldenValue = "{\"id\":\"\",\"session_id\":null,\"session\":null,\"schema\":[],\"data\":[],\"state\":\"Failed\",\"error\":{\"code\":1065,\"message\":\"error: \\n  --> SQL:1:8\\n  |\\n1 | select error\\n  |        ^^^^^ column doesn't exist\\n\\n\"},\"stats\":{\"scan_progress\":{\"rows\":0,\"bytes\":0},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":0,\"bytes\":0},\"running_time_ms\":0.0},\"affect\":null,\"stats_uri\":null,\"final_uri\":null,\"next_uri\":null,\"kill_uri\":null}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenValue);
        Assert.assertEquals(queryResults.getId(), "");
        Assert.assertEquals(queryResults.getSessionId(), null);
        Assert.assertEquals(queryResults.getSession(), null);
        Assert.assertEquals(queryResults.getState(), "Failed");
        Assert.assertEquals(queryResults.getError().getCode(), 1065);
        Assert.assertEquals(queryResults.getError().getMessage().contains("error: \n  --> SQL:1:8"), true);
    }

    @Test(groups = "unit")
    public void TestDateTime() {
        String goldenString = "{\"id\":\"1fbbaf5b-8807-47d3-bb9c-122a3b7c527c\",\"session_id\":\"ef4a4a66-7a81-4a90-b6ab-d484313111b8\",\"session\":{},\"schema\":[{\"name\":\"date\",\"type\":\"Date\"},{\"name\":\"ts\",\"type\":\"Timestamp\"}],\"data\":[[\"2022-04-07\",\"2022-04-07 01:01:01.123456\"],[\"2022-04-08\",\"2022-04-08 01:01:01.000000\"],[\"2022-04-07\",\"2022-04-07 01:01:01.123456\"],[\"2022-04-08\",\"2022-04-08 01:01:01.000000\"],[\"2022-04-07\",\"2022-04-07 01:01:01.123456\"],[\"2022-04-08\",\"2022-04-08 01:01:01.000000\"]],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":6,\"bytes\":72},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":6,\"bytes\":72},\"running_time_ms\":7.681399},\"affect\":null,\"stats_uri\":\"/v1/query/1fbbaf5b-8807-47d3-bb9c-122a3b7c527c\",\"final_uri\":\"/v1/query/1fbbaf5b-8807-47d3-bb9c-122a3b7c527c/final\",\"next_uri\":\"/v1/query/1fbbaf5b-8807-47d3-bb9c-122a3b7c527c/final\",\"kill_uri\":\"/v1/query/1fbbaf5b-8807-47d3-bb9c-122a3b7c527c/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "1fbbaf5b-8807-47d3-bb9c-122a3b7c527c");
        Assert.assertEquals(queryResults.getSessionId(), "ef4a4a66-7a81-4a90-b6ab-d484313111b8");
        Assert.assertEquals(queryResults.getSession().getDatabase(), null);
        Assert.assertEquals(queryResults.getSession().getKeepServerSessionSecs(), 0);
        Assert.assertEquals(queryResults.getSession().getSettings(), null);
        Assert.assertEquals(queryResults.getState(), "Succeeded");
        Assert.assertEquals(queryResults.getError(), null);
        Assert.assertEquals(queryResults.getSchema().size(), 2);
        Assert.assertEquals(queryResults.getSchema().get(0).getName(), "date");
        Assert.assertEquals(queryResults.getSchema().get(0).getDataType().getType(), "Date");
        Assert.assertEquals(queryResults.getSchema().get(1).getName(), "ts");
        Assert.assertEquals(queryResults.getSchema().get(1).getDataType().getType(), "Timestamp");
        for (List<Object> row : queryResults.getData()) {
            Assert.assertEquals(row.size(), 2);

        }
    }

    @Test(groups = "unit")
    public void TestUseDB() {
        String goldenString = "{\"id\":\"d0aa3285-0bf5-42da-b06b-0d3db55f10bd\",\"session_id\":\"ded852b7-0da2-46ba-8708-e6fcb1c33081\",\"session\":{\"database\":\"db2\"},\"schema\":[],\"data\":[],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":0,\"bytes\":0},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":0,\"bytes\":0},\"running_time_ms\":0.891883},\"affect\":{\"type\":\"UseDB\",\"name\":\"db2\"},\"stats_uri\":\"/v1/query/d0aa3285-0bf5-42da-b06b-0d3db55f10bd\",\"final_uri\":\"/v1/query/d0aa3285-0bf5-42da-b06b-0d3db55f10bd/final\",\"next_uri\":\"/v1/query/d0aa3285-0bf5-42da-b06b-0d3db55f10bd/final\",\"kill_uri\":\"/v1/query/d0aa3285-0bf5-42da-b06b-0d3db55f10bd/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "d0aa3285-0bf5-42da-b06b-0d3db55f10bd");
        QueryAffect affect = queryResults.getAffect();
        Assert.assertEquals(affect.getClass(), QueryAffect.UseDB.class);
        Assert.assertEquals(((QueryAffect.UseDB) affect).getName(), "db2");
    }

    @Test(groups = "unit")
    public void TestChangeSettings() {
        String goldenString = "{\"id\":\"a59cf8ff-f8a0-4bf6-bb90-120d3ea140c0\",\"session_id\":\"3423881e-f57b-4c53-a432-cf665ac1fb3e\",\"session\":{\"settings\":{\"max_threads\":\"1\"}},\"schema\":[],\"data\":[],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":0,\"bytes\":0},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":0,\"bytes\":0},\"running_time_ms\":0.81772},\"affect\":{\"type\":\"ChangeSettings\",\"keys\":[\"max_threads\"],\"values\":[\"1\"],\"is_globals\":[false]},\"stats_uri\":\"/v1/query/a59cf8ff-f8a0-4bf6-bb90-120d3ea140c0\",\"final_uri\":\"/v1/query/a59cf8ff-f8a0-4bf6-bb90-120d3ea140c0/final\",\"next_uri\":\"/v1/query/a59cf8ff-f8a0-4bf6-bb90-120d3ea140c0/final\",\"kill_uri\":\"/v1/query/a59cf8ff-f8a0-4bf6-bb90-120d3ea140c0/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        QueryAffect affect = queryResults.getAffect();
        Assert.assertEquals(affect.getClass(), QueryAffect.ChangeSettings.class);
        Assert.assertEquals(((QueryAffect.ChangeSettings) affect).getKeys().size(), 1);
        Assert.assertEquals(((QueryAffect.ChangeSettings) affect).getKeys().get(0), "max_threads");
        Assert.assertEquals(((QueryAffect.ChangeSettings) affect).getValues().size(), 1);
        Assert.assertEquals(((QueryAffect.ChangeSettings) affect).getValues().get(0), "1");
        Assert.assertEquals(((QueryAffect.ChangeSettings) affect).getIsGlobals().size(), 1);
        Assert.assertEquals(((QueryAffect.ChangeSettings) affect).getIsGlobals().get(0).booleanValue(), false);
    }


    @Test(groups = "unit")
    public void TestArray() {
        String goldenString = "{\"id\":\"eecb2440-0180-45cb-8b21-23f4a9975df3\",\"session_id\":\"ef692df6-657d-42b8-a10d-6e6cac657abe\",\"session\":{},\"schema\":[{\"name\":\"id\",\"type\":\"Int8\"},{\"name\":\"obj\",\"type\":\"Variant\"},{\"name\":\"d\",\"type\":\"Timestamp\"},{\"name\":\"s\",\"type\":\"String\"},{\"name\":\"arr\",\"type\":\"Array(Int64)\"}],\"data\":[[\"1\",\"{\\\"a\\\": 1,\\\"b\\\": 2}\",\"1983-07-12 21:30:55.888000\",\"hello world, 你好\",\"[1,2,3,4,5]\"]],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":1,\"bytes\":131},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":1,\"bytes\":131},\"running_time_ms\":9.827047},\"affect\":null,\"stats_uri\":\"/v1/query/eecb2440-0180-45cb-8b21-23f4a9975df3\",\"final_uri\":\"/v1/query/eecb2440-0180-45cb-8b21-23f4a9975df3/final\",\"next_uri\":\"/v1/query/eecb2440-0180-45cb-8b21-23f4a9975df3/final\",\"kill_uri\":\"/v1/query/eecb2440-0180-45cb-8b21-23f4a9975df3/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "eecb2440-0180-45cb-8b21-23f4a9975df3");
        Assert.assertEquals(queryResults.getSchema().size(), 5);
        Assert.assertEquals(queryResults.getSchema().get(0).getName(), "id");
        Assert.assertEquals(queryResults.getSchema().get(0).getDataType().getType(), "Int8");
    }

    @Test(groups = "unit")
    public void TestVariant() {
        String goldenString = "{\"id\":\"d74b2471-3a15-45e2-9ef4-ca8a39505661\",\"session_id\":\"f818e198-20d9-4c06-8de6-bc68ab6e9dc1\",\"session\":{},\"schema\":[{\"name\":\"var\",\"type\":\"Nullable(Variant)\"}],\"data\":[[\"1\"],[\"1.34\"],[\"true\"],[\"[1,2,3,[\\\"a\\\",\\\"b\\\",\\\"c\\\"]]\"],[\"{\\\"a\\\":1,\\\"b\\\":{\\\"c\\\":2}}\"]],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":5,\"bytes\":168},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":5,\"bytes\":168},\"running_time_ms\":7.827281},\"affect\":null,\"stats_uri\":\"/v1/query/d74b2471-3a15-45e2-9ef4-ca8a39505661\",\"final_uri\":\"/v1/query/d74b2471-3a15-45e2-9ef4-ca8a39505661/final\",\"next_uri\":\"/v1/query/d74b2471-3a15-45e2-9ef4-ca8a39505661/final\",\"kill_uri\":\"/v1/query/d74b2471-3a15-45e2-9ef4-ca8a39505661/kill\"}\n";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "d74b2471-3a15-45e2-9ef4-ca8a39505661");
        Assert.assertEquals(queryResults.getSchema().size(), 1);
        Assert.assertEquals(queryResults.getSchema().get(0).getName(), "var");
        Assert.assertEquals(queryResults.getSchema().get(0).getDataType().getType(), "Variant");
        Assert.assertEquals(queryResults.getSchema().get(0).getDataType().isNullable(), true);

    }
}
