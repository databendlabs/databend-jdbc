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

import java.math.BigInteger;
import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;

public class TestQueryResults {
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    // {"id":"1453cca7-f424-4009-974d-cdc93a264ec9","session_id":"34a28f84-2f2b-4740-904d-7c43bbb65b8e","session":{},"schema":{"fields":[{"name":"max(number)","default_expr":null,"data_type":{"type":"Nullable","inner":{"type":"UInt64"}}},{"name":"sum(number)","default_expr":null,"data_type":{"type":"Nullable","inner":{"type":"UInt64"}}}],"metadata":{}},"data":[["60","60"],["98","136"],["79","98"],["57","57"],["71","82"],["93","126"],["95","130"],["96","132"],["62","64"],["40","40"]],"state":"Succeeded","error":null,"stats":{"scan_progress":{"rows":100,"bytes":800},"write_progress":{"rows":0,"bytes":0},"result_progress":{"rows":10,"bytes":164},"running_time_ms":2.000744},"affect":null,"stats_uri":"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9","final_uri":"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/final","next_uri":"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/final","kill_uri":"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/kill"}
    @Test(groups = {"unit"})
    public void testBasic() {
        String goldenValue = "{\"id\":\"1453cca7-f424-4009-974d-cdc93a264ec9\",\"session_id\":\"34a28f84-2f2b-4740-904d-7c43bbb65b8e\","
        + "\"session\":{},\"schema\":{\"fields\":[{\"name\":\"max(number)\",\"default_expr\":null, " +
        "\"data_type\":{\"type\":\"Nullable\",\"inner\":{\"type\":\"UInt64\"}}},{\"name\":\"sum(number)\",\"default_expr\":null," +
        "\"data_type\":{\"type\":\"Nullable\",\"inner\":{\"type\":\"UInt64\"}}}],\"metadata\":{}}," +
        "\"data\":[[\"60\",\"60\"],[\"98\",\"136\"],[\"79\",\"98\"],[\"57\",\"57\"],[\"71\",\"82\"], " +
        "[\"93\",\"126\"],[\"95\",\"130\"],[\"96\",\"132\"],[\"62\",\"64\"],[\"40\",\"40\"]]," +
        "\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":100,\"bytes\":800}," +
        "\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":10,\"bytes\":164},\"running_time_ms\":2.000744}, " +
                "\"affect\":null,\"stats_uri\":\"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9\", " +
                "\"final_uri\":\"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/final\"," +
                "\"next_uri\":\"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/final\", " +
                "\"kill_uri\":\"/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/kill\"}\n";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenValue);
        Assert.assertEquals(queryResults.getId(), "1453cca7-f424-4009-974d-cdc93a264ec9");
        Assert.assertEquals(queryResults.getSessionId(), "34a28f84-2f2b-4740-904d-7c43bbb65b8e");
        Assert.assertEquals(queryResults.getSession().getDatabase(), null);
        Assert.assertEquals(queryResults.getSession().getKeepServerSessionSecs(), 0);
        Assert.assertEquals(queryResults.getSession().getSettings(), null);
        Assert.assertEquals(queryResults.getSchema().getFields().size(), 2);
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getName(), "max(number)");
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getDataType().getType(), "Nullable");
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getDataType().getInner().getType(), "UInt64");
        Assert.assertEquals(queryResults.getSchema().getFields().get(1).getName(), "sum(number)");
        Assert.assertEquals(queryResults.getSchema().getFields().get(1).getDataType().getType(), "Nullable");
        Assert.assertEquals(queryResults.getSchema().getFields().get(1).getDataType().getInner().getType(), "UInt64");
        for (List<Object> row : queryResults.getData()) {
            Assert.assertEquals(row.size(), 2);
            Assert.assertEquals(row.get(0).getClass(), BigInteger.class);
            Assert.assertEquals(row.get(1).getClass(), BigInteger.class);
        }
        Assert.assertEquals(queryResults.getState(), "Succeeded");
        Assert.assertEquals(queryResults.getError(), null);
        Assert.assertEquals(queryResults.getStats().getScanProgress().getRows(), 100);
        Assert.assertEquals(queryResults.getStats().getScanProgress().getBytes(), 800);
        Assert.assertEquals(queryResults.getStats().getWriteProgress().getRows(), 0);
        Assert.assertEquals(queryResults.getStats().getWriteProgress().getBytes(), 0);
        Assert.assertEquals(queryResults.getStats().getResultProgress().getRows(), 10);
        Assert.assertEquals(queryResults.getStats().getResultProgress().getBytes(), 164);
        Assert.assertEquals(Math.abs(queryResults.getStats().getRunningTimeMS() - 2.000744) < 0.000001, true);
        Assert.assertEquals(queryResults.getAffect(), null);
        Assert.assertEquals(queryResults.getStatsUri().toString(), "/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9");
        Assert.assertEquals(queryResults.getFinalUri().toString(), "/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/final");
        Assert.assertEquals(queryResults.getNextUri().toString(), "/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/final");
        Assert.assertEquals(queryResults.getKillUri().toString(), "/v1/query/1453cca7-f424-4009-974d-cdc93a264ec9/kill");
    }

    @Test(groups = "unit")
    public void TestError() {
        String goldenValue = "{\"id\":\"baeadb9c-c277-4fa7-9b3f-b439fb00075d\",\"session_id\":\"18edf21d-5d9f-4258-8781-0aafc5d41a83\",\"session\":{},\"schema\":{\"fields\":[],\"metadata\":{}},\"data\":[],\"state\":\"Failed\",\"error\":{\"code\":1006,\"message\":\"Incorrect CREATE query: required list of column descriptions or AS section or SELECT..\"},\"stats\":{\"scan_progress\":{\"rows\":0,\"bytes\":0},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":0,\"bytes\":0},\"running_time_ms\":1.086606},\"affect\":null,\"stats_uri\":\"/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d\",\"final_uri\":\"/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d/final\",\"next_uri\":\"/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d/final\",\"kill_uri\":\"/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenValue);
        Assert.assertEquals(queryResults.getId(), "baeadb9c-c277-4fa7-9b3f-b439fb00075d");
        Assert.assertEquals(queryResults.getSessionId(), "18edf21d-5d9f-4258-8781-0aafc5d41a83");
        Assert.assertEquals(queryResults.getSession().getDatabase(), null);
        Assert.assertEquals(queryResults.getSession().getKeepServerSessionSecs(), 0);
        Assert.assertEquals(queryResults.getSession().getSettings(), null);
        Assert.assertEquals(queryResults.getSchema().getFields().size(), 0);
        Assert.assertEquals(queryResults.getState(), "Failed");
        Assert.assertEquals(queryResults.getError().getCode(), 1006);
        Assert.assertEquals(queryResults.getError().getMessage(), "Incorrect CREATE query: required list of column descriptions or AS section or SELECT..");
        Assert.assertEquals(queryResults.getStats().getScanProgress().getRows(), 0);
        Assert.assertEquals(queryResults.getStats().getScanProgress().getBytes(), 0);
        Assert.assertEquals(queryResults.getStats().getWriteProgress().getRows(), 0);
        Assert.assertEquals(queryResults.getStats().getWriteProgress().getBytes(), 0);
        Assert.assertEquals(queryResults.getStats().getResultProgress().getRows(), 0);
        Assert.assertEquals(queryResults.getStats().getResultProgress().getBytes(), 0);
        Assert.assertEquals(Math.abs(queryResults.getStats().getRunningTimeMS() - 1.086606) < 0.000001, true);
        Assert.assertEquals(queryResults.getAffect(), null);
        Assert.assertEquals(queryResults.getStatsUri().toString(), "/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d");
        Assert.assertEquals(queryResults.getFinalUri().toString(), "/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d/final");
        Assert.assertEquals(queryResults.getNextUri().toString(), "/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d/final");
        Assert.assertEquals(queryResults.getKillUri().toString(), "/v1/query/baeadb9c-c277-4fa7-9b3f-b439fb00075d/kill");
    }

    @Test(groups = "unit")
    public void TestDateTime() {
        String goldenString = "{\"id\":\"86ffc1fd-d4e6-4b11-9001-c6bf61dbaf6d\",\"session_id\":\"7e255eba-2698-443e-a544-38c53af301d6\",\"session\":{},\"schema\":{\"fields\":[{\"name\":\"now()\",\"default_expr\":null,\"data_type\":{\"type\":\"Timestamp\"}}],\"metadata\":{}},\"data\":[[\"2022-12-23 12:27:04.081894\"]],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":1,\"bytes\":1},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":1,\"bytes\":8},\"running_time_ms\":1.3580409999999998},\"affect\":null,\"stats_uri\":\"/v1/query/86ffc1fd-d4e6-4b11-9001-c6bf61dbaf6d\",\"final_uri\":\"/v1/query/86ffc1fd-d4e6-4b11-9001-c6bf61dbaf6d/final\",\"next_uri\":\"/v1/query/86ffc1fd-d4e6-4b11-9001-c6bf61dbaf6d/final\",\"kill_uri\":\"/v1/query/86ffc1fd-d4e6-4b11-9001-c6bf61dbaf6d/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "86ffc1fd-d4e6-4b11-9001-c6bf61dbaf6d");
        Assert.assertEquals(queryResults.getSchema().getFields().size(), 1);
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getName(), "now()");
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getDataType().getType(), "Timestamp");
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getDataType().getInner(), null);
        for (List<Object> row : queryResults.getData()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.get(0).getClass(), String.class);
            Assert.assertEquals(row.get(0), "2022-12-23 12:27:04.081894");
        }
    }

    @Test(groups = "unit")
    public void TestUseDB() {
        String goldenString = "{\"id\":\"684a6d3c-e6d7-44b1-b8d6-3b687acb05a3\",\"session_id\":\"9d6018f2-10b1-4089-a1ac-0ee5b6068ca0\",\"session\":{\"database\":\"db1\"},\"schema\":{\"fields\":[],\"metadata\":{}},\"data\":[],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":0,\"bytes\":0},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":0,\"bytes\":0},\"running_time_ms\":2.46636},\"affect\":{\"type\":\"UseDB\",\"name\":\"db1\"},\"stats_uri\":\"/v1/query/684a6d3c-e6d7-44b1-b8d6-3b687acb05a3\",\"final_uri\":\"/v1/query/684a6d3c-e6d7-44b1-b8d6-3b687acb05a3/final\",\"next_uri\":\"/v1/query/684a6d3c-e6d7-44b1-b8d6-3b687acb05a3/final\",\"kill_uri\":\"/v1/query/684a6d3c-e6d7-44b1-b8d6-3b687acb05a3/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "684a6d3c-e6d7-44b1-b8d6-3b687acb05a3");
        QueryAffect affect = queryResults.getAffect();
        Assert.assertEquals(affect.getClass(), QueryAffect.UseDB.class);
        Assert.assertEquals(((QueryAffect.UseDB) affect).getName(), "db1");
    }

    @Test(groups = "unit")
    public void TestChangeSettings() {
        String goldenString = "{\"id\":\"068682a1-b61b-4dff-b059-5739d8bcc698\",\"session_id\":\"d1179f7d-5e7d-47a4-946b-184be2dcd670\",\"session\":{\"settings\":{\"max_threads\":\"1\"}},\"schema\":{\"fields\":[],\"metadata\":{}},\"data\":[],\"state\":\"Succeeded\",\"error\":null,\"stats\":{\"scan_progress\":{\"rows\":0,\"bytes\":0},\"write_progress\":{\"rows\":0,\"bytes\":0},\"result_progress\":{\"rows\":0,\"bytes\":0},\"running_time_ms\":0.61151},\"affect\":{\"type\":\"ChangeSettings\",\"keys\":[\"max_threads\"],\"values\":[\"1\"],\"is_globals\":[false]},\"stats_uri\":\"/v1/query/068682a1-b61b-4dff-b059-5739d8bcc698\",\"final_uri\":\"/v1/query/068682a1-b61b-4dff-b059-5739d8bcc698/final\",\"next_uri\":\"/v1/query/068682a1-b61b-4dff-b059-5739d8bcc698/final\",\"kill_uri\":\"/v1/query/068682a1-b61b-4dff-b059-5739d8bcc698/kill\"}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "068682a1-b61b-4dff-b059-5739d8bcc698");
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
    public void TestVariant() {
        String goldenString = "{\n" +
                "  \"id\": \"e6f3574e-c0f7-47b1-9e82-7050bee91e0b\",\n" +
                "  \"session_id\": \"9caf4553-0fd6-4649-b985-4c4ad6629829\",\n" +
                "  \"session\": {},\n" +
                "  \"schema\": {\n" +
                "    \"fields\": [\n" +
                "      {\n" +
                "        \"name\": \"parse_json('[-1, 12, 289, 2188, false]')\",\n" +
                "        \"default_expr\": null,\n" +
                "        \"data_type\": {\n" +
                "          \"type\": \"Variant\"\n" +
                "        }\n" +
                "      }\n" +
                "    ],\n" +
                "    \"metadata\": {}\n" +
                "  },\n" +
                "  \"data\": [\n" +
                "    [\n" +
                "      \"[-1,12,289,2188,false]\"\n" +
                "    ]\n" +
                "  ],\n" +
                "  \"state\": \"Succeeded\",\n" +
                "  \"error\": null,\n" +
                "  \"stats\": {\n" +
                "    \"scan_progress\": {\n" +
                "      \"rows\": 1,\n" +
                "      \"bytes\": 1\n" +
                "    },\n" +
                "    \"write_progress\": {\n" +
                "      \"rows\": 0,\n" +
                "      \"bytes\": 0\n" +
                "    },\n" +
                "    \"result_progress\": {\n" +
                "      \"rows\": 1,\n" +
                "      \"bytes\": 480\n" +
                "    },\n" +
                "    \"running_time_ms\": 2.086137\n" +
                "  },\n" +
                "  \"affect\": null,\n" +
                "  \"stats_uri\": \"/v1/query/e6f3574e-c0f7-47b1-9e82-7050bee91e0b\",\n" +
                "  \"final_uri\": \"/v1/query/e6f3574e-c0f7-47b1-9e82-7050bee91e0b/final\",\n" +
                "  \"next_uri\": \"/v1/query/e6f3574e-c0f7-47b1-9e82-7050bee91e0b/final\",\n" +
                "  \"kill_uri\": \"/v1/query/e6f3574e-c0f7-47b1-9e82-7050bee91e0b/kill\"\n" +
                "}";
        QueryResults queryResults = QUERY_RESULTS_CODEC.fromJson(goldenString);
        Assert.assertEquals(queryResults.getId(), "e6f3574e-c0f7-47b1-9e82-7050bee91e0b");
        Assert.assertEquals(queryResults.getSchema().getFields().size(), 1);
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getName(), "parse_json('[-1, 12, 289, 2188, false]')");
        Assert.assertEquals(queryResults.getSchema().getFields().get(0).getDataType().getType(), "Variant");
    }
}
