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


import org.jnosql.diana.api.Value;

import java.util.Objects;

/**
 * The implementation that uses {@link JSONGSONValue}
 */
public class JSONGSONValueProvider implements JSONValueProvider {

    @Override
    public Value of(String json) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(json, "Json is required");
        return JSONGSONValue.of(json);
    }

    @Override
    public Value of(byte[] json) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(json, "Json is required");
        return JSONGSONValue.of(String.valueOf(json));
    }

    @Override
    public String toJson(Object object) throws NullPointerException, UnsupportedOperationException {
        return JSONGSONValue.GSON.toJson(Objects.requireNonNull(object, "object is required"));
    }

    @Override
    public byte[] toJsonArray(Object object) throws NullPointerException, UnsupportedOperationException {
        return toJson(object).getBytes();
    }
}
