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
package org.eclipse.jnosql.communication.elasticsearch.document;


import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentEntity;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.eclipse.jnosql.communication.elasticsearch.document.EntityConverter.ENTITY;
import static org.eclipse.jnosql.communication.elasticsearch.document.EntityConverter.ID_FIELD;

class ElasticsearchEntry {

    private final String id;

    private final Map<String, Object> map;

    private final String collection;

    private static final Function<Map.Entry<?, ?>, Document> ENTRY_DOCUMENT = entry ->
            Document.of(entry.getKey().toString(), entry.getValue());


    private ElasticsearchEntry(String id, Map<String, Object> map) {
        this.id = id;
        this.collection = map == null ? null : map.getOrDefault(ENTITY, "_doc").toString();
        this.map = map;
    }

    boolean isEmpty() {
        return isNull(id) || isNull(collection) || isNull(map);
    }

    boolean isNotEmpty() {
        return !isEmpty();
    }

    DocumentEntity toEntity() {
        Document id = Document.of(ID_FIELD, this.id);
        List<Document> documents = map.keySet().stream()
                .map(k -> toDocument(k, map))
                .collect(Collectors.toList());
        DocumentEntity entity = DocumentEntity.of(collection, documents);
        entity.remove(ID_FIELD);
        entity.add(id);
        return entity;
    }

    private Document toDocument(String key, Map<String, Object> properties) {
        Object value = properties.get(key);
        if (Map.class.isInstance(value)) {
            Map map = Map.class.cast(value);
            return Document.of(key, map.keySet()
                    .stream().map(k -> toDocument(k.toString(), map))
                    .collect(Collectors.toList()));
        }
        if (isADocumentIterable(value)) {
            List<List<Document>> documents = new ArrayList<>();
            for (Object object : Iterable.class.cast(value)) {
                Map<?, ?> map = Map.class.cast(object);
                documents.add(map.entrySet().stream().map(ENTRY_DOCUMENT).collect(toList()));
            }
            return Document.of(key, documents);

        }
        return Document.of(key, value);
    }

    private boolean isADocumentIterable(Object value) {
        return Iterable.class.isInstance(value) &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(Map.class::isInstance);
    }


    static ElasticsearchEntry of(SearchHit searchHit) {
        return new ElasticsearchEntry(searchHit.getId(),
                searchHit.getSourceAsMap());
    }

    static ElasticsearchEntry of(GetResponse searchHit) {
        return new ElasticsearchEntry(searchHit.getId(),
                searchHit.getSourceAsMap());
    }
}
