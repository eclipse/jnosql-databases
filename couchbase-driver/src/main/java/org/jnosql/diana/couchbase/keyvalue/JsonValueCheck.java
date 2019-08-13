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
package org.jnosql.diana.couchbase.keyvalue;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

final class JsonValueCheck {

    private static final List<Class<?>> CLASSES;

    static {

        CLASSES = new ArrayList<>();
        CLASSES.add(String.class);
        CLASSES.add(Integer.class);
        CLASSES.add(Long.class);
        CLASSES.add(Double.class);
        CLASSES.add(Boolean.class);
        CLASSES.add(BigInteger.class);
        CLASSES.add(BigDecimal.class);
        CLASSES.add(JsonObject.class);
        CLASSES.add(JsonArray.class);
    }

    private JsonValueCheck() {
    }

    public static boolean checkType(Class<?> type) {
        return CLASSES.contains(type);
    }
}
