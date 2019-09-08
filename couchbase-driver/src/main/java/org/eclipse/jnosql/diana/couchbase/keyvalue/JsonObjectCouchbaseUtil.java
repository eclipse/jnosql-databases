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
package org.eclipse.jnosql.diana.couchbase.keyvalue;

import com.couchbase.client.java.document.json.JsonObject;

import javax.json.bind.Jsonb;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

final class JsonObjectCouchbaseUtil {

    private static final Type TYPE = new HashMap<String, Object>() {
    }.getClass().getGenericSuperclass();

    private JsonObjectCouchbaseUtil() {
    }

    static JsonObject toJson(Jsonb jsonb, Object value) {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        jsonb.toJson(value, stream);
        InputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
        Map<String, ?> map = jsonb.fromJson(inputStream, TYPE);

        return JsonObject.from(map);

    }
}
