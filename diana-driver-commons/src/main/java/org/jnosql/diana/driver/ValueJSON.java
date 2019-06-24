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
package org.jnosql.diana.driver;

import jakarta.nosql.TypeSupplier;
import jakarta.nosql.Value;

import javax.json.bind.Jsonb;
import java.util.Objects;


/**
 * A {@link Value} implementation that storage all the information as a {@link String} with JSON format.
 */
public class ValueJSON implements Value {

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();

    private final String json;

    ValueJSON(String json) {
        this.json = json;
    }


    @Override
    public Object get() {
        return json;
    }

    @Override
    public <T> T get(Class<T> clazz) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(clazz, "clazz is required");
        return JSONB.fromJson(json, clazz);
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(typeSupplier, "typeSupplier is required");
        return JSONB.fromJson(json, typeSupplier.get());
    }

    @Override
    public boolean isInstanceOf(Class<?> typeClass) {
        Objects.requireNonNull(typeClass, "typeClass is requried");
        return typeClass.isInstance(json);
    }

    /**
     * Returns a new instance of {@link Value} keeping the value as JSON
     *
     * @param json the value
     * @return the new Value instance
     * @throws NullPointerException when json is null
     */
    public static Value of(String json) throws NullPointerException {
        Objects.requireNonNull(json, "json is required");
        return new ValueJSON(json);
    }

    /**
     * Returns a new instance of {@link Value} converting to JSON first
     *
     * @param json the value
     * @return the new Value instance
     * @throws NullPointerException when json is null
     */
    public static Value of(Object json) throws NullPointerException {
        Objects.requireNonNull(json, "json is required");
        return new ValueJSON(JSONB.toJson(json));
    }
}
