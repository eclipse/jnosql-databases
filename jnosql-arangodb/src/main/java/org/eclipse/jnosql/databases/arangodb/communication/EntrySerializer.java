/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.arangodb.communication;

import com.fasterxml.jackson.databind.JsonSerializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

record EntrySerializer<T> (JsonSerializer<T> serializer, Class<T> type) {





    static <T> EntrySerializer<T> of(String name) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {

        Objects.requireNonNull(name, "name is required");
        Class<?> type = Class.forName(name);
        if(!type.isAssignableFrom(JsonSerializer.class)) {
            throw new IllegalArgumentException(String.format("The class %s should extends JsonSerializer", name));
        }
        Object instance = type.getConstructor().newInstance();

        return null;
    }

}
