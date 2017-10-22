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
package org.jnosql.diana.mongodb.document;

import org.bson.Document;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.driver.ValueUtil;

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
    private static final Function<Object, String> KEY_DOCUMENT = d -> cast(d).getName();
    private static final Function<Object, Object> VALUE_DOCUMENT = d -> cast(d).get();

    private static final Function<Map.Entry<?, ?>, org.jnosql.diana.api.document.Document> ENTRY_DOCUMENT = entry ->
            org.jnosql.diana.api.document.Document.of(entry.getKey().toString(), entry.getValue());


    private MongoDBUtils() {
    }

    static Document getDocument(DocumentEntity entity) {
        Document document = new Document();
        entity.getDocuments().stream().forEach(d -> document.append(d.getName(), convert(d.getValue())));
        return document;
    }

    private static Object convert(Value value) {
        Object val = ValueUtil.convert(value);
        if (val instanceof org.jnosql.diana.api.document.Document) {
            org.jnosql.diana.api.document.Document subDocument = (org.jnosql.diana.api.document.Document) val;
            Object converted = convert(subDocument.getValue());
            return new Document(subDocument.getName(), converted);
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


    public static List<org.jnosql.diana.api.document.Document> of(Map<String, ?> values) {
        Predicate<String> isNotNull = s -> values.get(s) != null;
        Function<String, org.jnosql.diana.api.document.Document> documentMap = key -> {
            Object value = values.get(key);
            if (value instanceof Document) {
                return org.jnosql.diana.api.document.Document.of(key, of(Document.class.cast(value)));
            } else if (isDocumentIterable(value)) {
                List<List<org.jnosql.diana.api.document.Document>> documents = new ArrayList<>();
                for (Object object : Iterable.class.cast(value)) {
                    Map<?, ?> map = Map.class.cast(object);
                    documents.add(map.entrySet().stream().map(ENTRY_DOCUMENT).collect(toList()));
                }
                return org.jnosql.diana.api.document.Document.of(key, documents);
            }
            return org.jnosql.diana.api.document.Document.of(key, Value.of(value));
        };
        return values.keySet().stream().filter(isNotNull).map(documentMap).collect(Collectors.toList());
    }

    private static boolean isDocumentIterable(Object value) {
        return value instanceof Iterable &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(v -> Document.class.isInstance(v));
    }

    private static Object getMap(Object val) {
        return StreamSupport.stream(Iterable.class.cast(val).spliterator(), false)
                .collect(toMap(KEY_DOCUMENT, VALUE_DOCUMENT));
    }

    private static org.jnosql.diana.api.document.Document cast(Object document) {
        return org.jnosql.diana.api.document.Document.class.cast(document);
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> org.jnosql.diana.api.document.Document.class.isInstance(d));
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }
}
