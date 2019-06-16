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
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentEntity;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

final class EntityConverter {

    static final String ID_FIELD = "_id";
    static final String KEY_FIELD = "_key";
    static final String SPLIT_KEY = ":";
    static final char SPLIT_KEY_CHAR = ':';

    private EntityConverter() {
    }

    static List<DocumentEntity> convert(Collection<String> keys, Bucket bucket) {
        return keys
                .stream()
                .map(bucket::get)
                .filter(Objects::nonNull)
                .map(j -> {
                    List<Document> documents = toDocuments(j.content().toMap());
                    return DocumentEntity.of(j.id().split(SPLIT_KEY)[0], documents);
                })
                .collect(Collectors.toList());
    }

    static List<DocumentEntity> convert(N1qlQueryResult result, String database) {
        return result.allRows().stream()
                .map(N1qlQueryRow::value)
                .map(JsonObject::toMap)
                .map(m -> m.get(database))
                .filter(Objects::nonNull)
                .filter(Map.class::isInstance)
                .map(m -> (Map<String, Object>) m)
                .map(map -> {
                    List<Document> documents = toDocuments(map);
                    Optional<Document> keyDocument = documents.stream().filter(d -> KEY_FIELD.equals(d.getName())).findFirst();
                    String collection = keyDocument.map(d -> d.get(String.class)).orElse(database).split(SPLIT_KEY)[0];
                    return DocumentEntity.of(collection, documents);
                }).collect(toList());
    }

    static String getPrefix(Document document, String collection) {
        String id = document.get(String.class);
        return getPrefix(collection, id);
    }

    private static List<Document> toDocuments(Map<String, Object> map) {
        List<Document> documents = new ArrayList<>();
        for (String key : map.keySet()) {
            Object value = map.get(key);
            if (Map.class.isInstance(value)) {
                documents.add(Document.of(key, toDocuments(Map.class.cast(value))));
            } else if (isADocumentIterable(value)) {
                List<List<Document>> subDocuments = new ArrayList<>();
                stream(Iterable.class.cast(value).spliterator(), false)
                        .map(m -> toDocuments(Map.class.cast(m)))
                        .forEach(e -> subDocuments.add((List<Document>) e));
                documents.add(Document.of(key, subDocuments));
            } else {
                documents.add(Document.of(key, value));
            }
        }
        return documents;
    }

    private static boolean isADocumentIterable(Object value) {
        return Iterable.class.isInstance(value) &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(Map.class::isInstance);
    }

    static String getPrefix(String collection, String id) {
        String[] ids = id.split(SPLIT_KEY);
        if (ids.length == 2 && collection.equals(ids[0])) {
            return id;
        }
        return collection + SPLIT_KEY_CHAR + id;
    }


    static JsonObject convert(DocumentEntity entity) {
        requireNonNull(entity, "entity is required");

        JsonObject jsonObject = JsonObject.create();
        entity.getDocuments().stream()
                .forEach(toJsonObject(jsonObject));
        return jsonObject;
    }

    private static Consumer<Document> toJsonObject(JsonObject jsonObject) {
        return d -> {
            Object value = ValueUtil.convert(d.getValue());
            if (Document.class.isInstance(value)) {
                convertDocument(jsonObject, d, value);
            } else if (Iterable.class.isInstance(value)) {
                convertIterable(jsonObject, d, value);
            } else {
                jsonObject.put(d.getName(), value);
            }
        };
    }


    private static void convertDocument(JsonObject jsonObject, Document d, Object value) {
        Document document = Document.class.cast(value);
        jsonObject.put(d.getName(), Collections.singletonMap(document.getName(), document.get()));
    }

    private static void convertIterable(JsonObject jsonObject, Document document, Object value) {
        JsonObject map = JsonObject.create();
        JsonArray array = JsonArray.create();
        Iterable.class.cast(value).forEach(element -> {
            if (Document.class.isInstance(element)) {
                Document subdocument = Document.class.cast(element);
                map.put(subdocument.getName(), subdocument.get());
            } else if (isSudDocument(element)) {
                JsonObject subJson = JsonObject.create();

                stream(Iterable.class.cast(element).spliterator(), false)
                        .forEach(getSubdocument(subJson));
                array.add(subJson);
            } else {
                array.add(element);
            }
        });
        if (array.isEmpty()) {
            jsonObject.put(document.getName(), map);
        } else {
            jsonObject.put(document.getName(), array);
        }
    }

    private static Consumer getSubdocument(JsonObject subJson) {
        return e -> toJsonObject(subJson).accept((Document) e);
    }


    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(org.jnosql.diana.api.document.Document.class::isInstance);
    }


}
