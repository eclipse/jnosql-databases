/*
 *  Copyright (c) 2017 Otávio Santana and others
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Apache License v2.0 which accompanies this distribution.
 *   The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *   and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 *   You may elect to redistribute this code under either of these licenses.
 *
 *   Contributors:
 *
 *   Otavio Santana
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
