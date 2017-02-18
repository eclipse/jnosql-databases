/*
 * Copyright 2017 Otavio Santana and others
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
import org.jnosql.diana.api.TypeSupplier;
import org.jnosql.diana.api.Value;

import java.util.Objects;

/**
 * A {@link Value} implementation that storage all the information as a {@link String} JSON.
 * This implementation uses {@link Gson} as converter
 */
final class JSONGSONValue implements Value {

    static final Gson GSON = new Gson();

    private final Gson gson;

    private final String json;

    private JSONGSONValue(Gson gson, String json) {
        this.gson = gson;
        this.json = json;
    }

    public static Value of(String json) {
        Objects.requireNonNull(json, "json is required");
        return new JSONGSONValue(GSON, json);
    }

    public static Value of(Object value) {
        Objects.requireNonNull(value, "value is required");
        return of(GSON.toJson(value));
    }

    @Override
    public Object get() {
        return json;
    }


    @Override
    public <T> T get(Class<T> clazz) throws NullPointerException, UnsupportedOperationException {
        return gson.fromJson(json, clazz);
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        return gson.fromJson(json, new TypeToken<T>() {
        }.getType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!Value.class.isInstance(o)) {
            return false;
        }
        Value that = (Value) o;
        return Objects.equals(json, that.get(String.class));
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
