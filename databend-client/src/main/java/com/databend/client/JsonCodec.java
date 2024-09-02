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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JsonCodec<T> {
    // copy of https://github.com/airlift/airlift/blob/master/json/src/main/java/io/airlift/json/ObjectMapperProvider.java
    static final Supplier<ObjectMapper> OBJECT_MAPPER_SUPPLIER = () -> new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(MapperFeature.AUTO_DETECT_CREATORS)
            .disable(MapperFeature.AUTO_DETECT_FIELDS)
            .disable(MapperFeature.AUTO_DETECT_SETTERS)
            .disable(MapperFeature.AUTO_DETECT_GETTERS)
            .disable(MapperFeature.AUTO_DETECT_IS_GETTERS)
            .disable(MapperFeature.USE_GETTERS_AS_SETTERS)
            .disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
            .disable(MapperFeature.INFER_PROPERTY_MUTATORS)
            .disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
            .registerModule(new Jdk8Module()).registerModule(new SimpleModule().addDeserializer(
                    DiscoveryResponseCodec.DiscoveryResponse.class, new DiscoveryResponseCodec.DiscoveryResponseDeserializer()
            ));

    private final ObjectMapper mapper;
    private final Type type;
    private final JavaType javaType;

    private JsonCodec(ObjectMapper mapper, Type type) {
        this.mapper = requireNonNull(mapper, "mapper is null");
        this.type = requireNonNull(type, "type is null");
        this.javaType = mapper.getTypeFactory().constructType(type);
    }

    public static <T> JsonCodec<T> jsonCodec(Class<T> type) {
        return new JsonCodec<>(OBJECT_MAPPER_SUPPLIER.get(), type);
    }

    public Type getType() {
        return type;
    }

    public T fromJson(String json)
            throws JsonProcessingException {
        try (JsonParser parser = mapper.createParser(json)) {
            T value = mapper.readerFor(javaType).readValue(parser);
            checkArgument(parser.nextToken() == null, "Found characters after the expected end of input");
            return value;
        } catch (JsonProcessingException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public T fromJson(InputStream inputStream)
            throws IOException, JsonProcessingException {
        try (JsonParser parser = mapper.createParser(inputStream)) {
            T value = mapper.readerFor(javaType).readValue(parser);
            checkArgument(parser.nextToken() == null, "Found characters after the expected end of input");
            return value;
        }
    }
}
