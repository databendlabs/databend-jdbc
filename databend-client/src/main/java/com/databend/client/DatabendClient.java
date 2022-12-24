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

import okhttp3.Request;

import java.io.Closeable;

public interface DatabendClient extends Closeable {
    String getQuery();
    @Override
    void close();

    DatabendSession getSession();
    QueryResults getResults();
    // execute Restful query request for the first time.
    // @param request the request to be executed
    // @return true if request finished with result
    // @throws requestFailedException if the request failed
    boolean execute(Request request);

    // get the next page of the query result
    // @return true if there is next page or no more result
    boolean next();

    boolean isRunning();

}
