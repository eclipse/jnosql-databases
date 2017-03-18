/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.elasticsearch.document;


import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;

class ElasticsearchEntry {

    private final String id;

    private final Map<String, Object> map;

    private final String collection;

    ElasticsearchEntry(String id, String collection, Map<String, Object> map) {
        this.id = id;
        this.collection = collection;
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
            List<Document> documents = new ArrayList<>();
            for (Object object : Iterable.class.cast(value)) {
                documents.add(Document.of(Map.class.cast(object).get("name").toString(),
                        Map.class.cast(Map.class.cast(object).get("value")).get("value")));
            }
            return Document.of(key, documents);

        }
        return Document.of(key, value);
    }

    private boolean isADocumentIterable(Object value) {
        return Iterable.class.isInstance(value) &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(d -> Map.class.isInstance(d));
    }

}
