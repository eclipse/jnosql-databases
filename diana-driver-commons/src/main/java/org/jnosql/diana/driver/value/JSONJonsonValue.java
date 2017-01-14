/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.driver.value;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.johnzon.mapper.Mapper;
import org.apache.johnzon.mapper.MapperBuilder;
import org.jnosql.diana.api.TypeSupplier;
import org.jnosql.diana.api.Value;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Objects;

/**
 * A {@link Value} implementation that storage all the information as a {@link String} JSON.
 * This implementation uses {@link Gson} as converter
 */
public final class JSONJonsonValue implements Value {

    private static Mapper MAPPER = new MapperBuilder().build();

    private final Mapper mapper;

    private final String json;


    private JSONJonsonValue(Mapper mapper, String json) {
        this.mapper = mapper;
        this.json = json;
    }

    public static Value of(String json) {
        return new JSONJonsonValue(MAPPER, json);
    }

    @Override
    public Object get() {
        return json;
    }


    @Override
    public <T> T get(Class<T> clazz) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(clazz, "clazz is required");
        InputStream stream = new ByteArrayInputStream(json.getBytes());
        return mapper.readObject(stream, clazz);
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        mapper.readObject()
        return gson.fromJson(json, new TypeToken<T>() {
        }.getType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JSONJonsonValue that = (JSONJonsonValue) o;
        return Objects.equals(json, that.json);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(json);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JSONGSONValue{");
        sb.append("gson=").append(gson);
        sb.append(", json='").append(json).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
