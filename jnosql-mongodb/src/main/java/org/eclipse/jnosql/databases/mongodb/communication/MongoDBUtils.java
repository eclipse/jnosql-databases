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
package org.eclipse.jnosql.databases.mongodb.communication;

import org.bson.Document;
import org.bson.types.Binary;
import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

final class MongoDBUtils {
    static final String ID_FIELD = "_id";

    private static final Function<Object, String> KEY_DOCUMENT = d -> cast(d).name();
    private static final Function<Object, Object> VALUE_DOCUMENT = d -> MongoDBUtils.convert(cast(d).value());

    private MongoDBUtils() {
    }

    static Document getDocument(DocumentEntity entity) {
        Document document = new Document();
        entity.documents().forEach(d -> document.append(d.name(), convert(d.value())));
        return document;
    }

    private static Object convert(Value value) {
        Object val = ValueUtil.convert(value);
        if (val instanceof org.eclipse.jnosql.communication.document.Document) {
            org.eclipse.jnosql.communication.document.Document subDocument = (org.eclipse.jnosql.communication.document.Document) val;
            Object converted = convert(subDocument.value());
            return new Document(subDocument.name(), converted);
        }
        if (isSudDocument(val)) {
            return getMap(val);
        }
        if (isSudDocumentList(val)) {
            return StreamSupport.stream(Iterable.class.cast(val).spliterator(), false)
                    .map(MongoDBUtils::getMap).collect(toList());
        }
        return val;
    }


    public static List<org.eclipse.jnosql.communication.document.Document> of(Map<String, ?> values) {
        Predicate<String> isNotNull = s -> values.get(s) != null;
        Function<String, org.eclipse.jnosql.communication.document.Document> documentMap = key -> {
            Object value = values.get(key);
            return getDocument(key, value);
        };
        return values.keySet().stream().filter(isNotNull).map(documentMap).collect(Collectors.toList());
    }

    private static org.eclipse.jnosql.communication.document.Document getDocument(String key, Object value) {
        if (value instanceof Document) {
            return org.eclipse.jnosql.communication.document.Document.of(key, of(Document.class.cast(value)));
        } else if (isDocumentIterable(value)) {
            List<List<org.eclipse.jnosql.communication.document.Document>> documents = new ArrayList<>();
            for (Object object : Iterable.class.cast(value)) {
                Map<?, ?> map = Map.class.cast(object);
                documents.add(map.entrySet().stream().map(e -> getDocument(e.getKey().toString(), e.getValue())).collect(toList()));
            }
            return org.eclipse.jnosql.communication.document.Document.of(key, documents);
        }

        return org.eclipse.jnosql.communication.document.Document.of(key, Value.of(convertValue(value)));
    }

    private static Object convertValue(Object value) {
        if (value instanceof Binary) {
            return Binary.class.cast(value).getData();
        }
        return value;
    }

    private static boolean isDocumentIterable(Object value) {
        return value instanceof Iterable &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(Document.class::isInstance);
    }

    private static Object getMap(Object val) {
        return StreamSupport.stream(Iterable.class.cast(val).spliterator(), false)
                .collect(toMap(KEY_DOCUMENT, VALUE_DOCUMENT));
    }

    private static org.eclipse.jnosql.communication.document.Document cast(Object document) {
        return org.eclipse.jnosql.communication.document.Document.class.cast(document);
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(org.eclipse.jnosql.communication.document.Document.class::isInstance);
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }
}
