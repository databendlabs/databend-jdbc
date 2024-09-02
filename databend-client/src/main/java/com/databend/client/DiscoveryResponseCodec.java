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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DiscoveryResponseCodec {

    public static class DiscoveryResponse {
        private final List<DiscoveryNode> nodes;
        private final QueryErrors error;

        public DiscoveryResponse(List<DiscoveryNode> nodes, QueryErrors error) {
            this.nodes = nodes;
            this.error = error;
        }

        // Getters for nodes and error
        public List<DiscoveryNode> getNodes() {
            return nodes;
        }

        public QueryErrors getError() {
            return error;
        }
    }


    public static class DiscoveryResponseDeserializer extends StdDeserializer<DiscoveryResponse> {

        public DiscoveryResponseDeserializer() {
            this(null);
        }

        public DiscoveryResponseDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public DiscoveryResponse deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            ObjectMapper mapper = (ObjectMapper) jp.getCodec();
            JsonNode rootNode = mapper.readTree(jp);

            List<DiscoveryNode> nodes = new ArrayList<>();
            QueryErrors error = null;

            if (rootNode.has("error")) {
                // Deserialize error
                error = mapper.treeToValue(rootNode.get("error"), QueryErrors.class);
            } else if (rootNode.isArray()) {
                // Deserialize nodes
                for (JsonNode element : rootNode) {
                    DiscoveryNode node = new DiscoveryNode(element.get("address").asText());
                    nodes.add(node);
                }
            } else {
                throw new JsonProcessingException("Unrecognized JSON format") {
                };
            }

            return new DiscoveryResponse(nodes, error);
        }
    }

    // Method to deserialize from JSON string
    public static <T> T fromJson(String json, Class<T> valueType) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(DiscoveryResponse.class, new DiscoveryResponseDeserializer());
        objectMapper.registerModule(module);
        return objectMapper.readValue(json, valueType);
    }
}
