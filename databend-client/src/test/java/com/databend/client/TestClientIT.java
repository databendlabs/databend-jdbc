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

import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.databend.client.ClientSettings.*;

@Test(timeOut = 10000)
public class TestClientIT {
    static String port = System.getenv("DATABEND_TEST_CONN_PORT") != null ? System.getenv("DATABEND_TEST_CONN_PORT").trim() : "8000";

    // please setup a local databend cluster before running this test, and create databend
    private static final String DATABEND_HOST = "http://databend:databend@127.0.0.1:" + port;
    private static final String DATABASE = "default";

    @Test(groups = {"it"})
    public void testBasicQueryPagination() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(OkHttpUtils.basicAuthInterceptor("databend", "databend")).build();

        ClientSettings settings = new ClientSettings(DATABEND_HOST);
        AtomicReference<String> lastNodeID = new AtomicReference<>();
        DatabendClient cli = new DatabendClientV1(client, "select 1", settings, null, lastNodeID);
        System.out.println(cli.getResults().getData());
        Assert.assertEquals(cli.getQuery(), "select 1");
        Assert.assertEquals(cli.getSession().getDatabase(), DATABASE);
        Assert.assertNotNull(cli.getResults());
        Assert.assertEquals(cli.getResults().getSchema().size(), 1);
        for (List<Object> data : cli.getResults().getData()) {
            Assert.assertEquals(data.size(), 1);
            Assert.assertEquals((Short) data.get(0), Short.valueOf("1"));
        }
        cli.close();
    }

    @Test(groups = {"it"})
    public void testConnectionRefused() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(OkHttpUtils.basicAuthInterceptor("databend", "databend")).build();
        ClientSettings settings = new ClientSettings("http://localhost:13191");

        AtomicReference<String> lastNodeID = new AtomicReference<>();

        try {
            DatabendClient cli = new DatabendClientV1(client, "select 1", settings, null, lastNodeID);
            cli.getResults(); // This should trigger the connection attempt
            Assert.fail("Expected exception was not thrown");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(
                    e instanceof ConnectException || e.getCause() instanceof ConnectException, "Exception should be ConnectionException or contain ConnectionException as cause");

        }
    }

    @Test(groups = {"it"})
    public void testBasicQueryIDHeader() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(OkHttpUtils.basicAuthInterceptor("databend", "databend")).build();
        String expectedUUID = UUID.randomUUID().toString().replace("-","");
        AtomicReference<String> lastNodeID = new AtomicReference<>();

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put(X_Databend_Query_ID, expectedUUID);
        ClientSettings settings = new ClientSettings(DATABEND_HOST, DatabendSession.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(), additionalHeaders, null, DEFAULT_RETRY_ATTEMPTS);
        DatabendClient cli = new DatabendClientV1(client, "select 1", settings, null, lastNodeID);
        Assert.assertEquals(cli.getAdditionalHeaders().get(X_Databend_Query_ID), expectedUUID);

        String expectedUUID1 = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> additionalHeaders1 = new HashMap<>();
        additionalHeaders1.put(X_Databend_Query_ID, expectedUUID1);
        ClientSettings settings1 = new ClientSettings(DATABEND_HOST, DatabendSession.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(), additionalHeaders1, null, DEFAULT_RETRY_ATTEMPTS);
        Assert.assertEquals(cli.getAdditionalHeaders().get(X_Databend_Query_ID), expectedUUID);
        // check X_Databend_Query_ID won't change after calling next()
        DatabendClient cli1 = new DatabendClientV1(client, "SELECT number from numbers(200000) order by number", settings1, null, lastNodeID);
        for (int i = 1; i < 1000; i++) {
            cli.advance();
            Assert.assertEquals(cli1.getAdditionalHeaders().get(X_Databend_Query_ID), expectedUUID1);
        }
        System.out.println(cli1.getResults().getData());
        System.out.println(cli1.getAdditionalHeaders());
        Assert.assertEquals(cli1.getAdditionalHeaders().get(X_Databend_Query_ID), expectedUUID1);
    }

    @Test(groups = {"it"})
    public void testDiscoverNodes() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(OkHttpUtils.basicAuthInterceptor("databend", "databend")).build();
        String expectedUUID = UUID.randomUUID().toString().replace("-", "");

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put(X_Databend_Query_ID, expectedUUID);
        ClientSettings settings = new ClientSettings(DATABEND_HOST, DatabendSession.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(), additionalHeaders, null, DEFAULT_RETRY_ATTEMPTS);
        List<DiscoveryNode> nodes = DatabendClientV1.discoverNodes(client, settings);
        Assert.assertFalse(nodes.isEmpty());
        for (DiscoveryNode node : nodes) {
            System.out.println(node.getAddress());
        }
    }

    @Test(groups = {"it"})
    public void testDiscoverNodesUnSupported() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(OkHttpUtils.basicAuthInterceptor("databend", "databend")).build();
        String expectedUUID = UUID.randomUUID().toString().replace("-", "");

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put(X_Databend_Query_ID, expectedUUID);
        additionalHeaders.put("~mock.unsupported.discovery", "true");
        ClientSettings settings = new ClientSettings(DATABEND_HOST, DatabendSession.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(), additionalHeaders, null, DEFAULT_RETRY_ATTEMPTS);
        try {
            DatabendClientV1.discoverNodes(client, settings);
            Assert.fail("Expected exception was not thrown");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof UnsupportedOperationException, "Exception should be UnsupportedOperationException");
        }
    }
}
