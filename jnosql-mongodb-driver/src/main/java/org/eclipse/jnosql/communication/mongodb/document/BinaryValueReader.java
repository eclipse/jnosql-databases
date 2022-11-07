/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.mongodb.document;

import jakarta.nosql.ValueReader;
import org.bson.types.Binary;

/**
 * An implementation of {@link ValueReader} of {@link Binary}
 */
public class BinaryValueReader implements ValueReader {

    @Override
    public boolean test(Class<?> type) {
        return Binary.class.equals(type);
    }

    @Override
    public <T> T read(Class<T> valueType, Object value) {

        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (T) new Binary((byte[]) value);
        }
        return (T) new Binary(value.toString().getBytes());
    }
}
