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

import java.util.List;

public class TestClientIT
{
    // please setup a local databend cluster before running this test
    private static final String DATABEND_HOST = "http://0.0.0.0:8000";
    private static final String DATABASE = "default";
    @Test( groups = {"it"} )
    public void testBasicQueryPagination() {
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(OkHttpUtils.basicAuthInterceptor("root", "root")).build();

        ClientSettings settings = new ClientSettings(DATABEND_HOST);
        DatabendClient cli = new DatabendClientV1(client, "select 1", settings);
        Assert.assertEquals(cli.getQuery(), "select 1");
        Assert.assertEquals(cli.getSession().getDatabase(), DATABASE);
        Assert.assertNotNull(cli.getResults());
        Assert.assertEquals(cli.getResults().getSchema().getFields().size(), 1);
        for (List<Object> data: cli.getResults().getData()) {
            Assert.assertEquals(data.size(), 1);
            Assert.assertEquals((Short)data.get(0), Short.valueOf("1"));
        }
        cli.close();
    }

}
