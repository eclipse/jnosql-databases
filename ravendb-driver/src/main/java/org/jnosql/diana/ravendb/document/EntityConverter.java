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
package org.jnosql.diana.ravendb.document;

import net.ravendb.client.Constants;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

final class EntityConverter {

    static final String ID_FIELD = "_id";


    private EntityConverter() {
    }


    public static  String getId(Map entity) {
        Map<String, Object> metadata = (Map<String, Object>) entity.remove(Constants.Documents.Metadata.KEY);
        return (String) metadata.get(Constants.Documents.Metadata.ID);
    }

    static DocumentEntity getEntity(Map map) {

        Map<String, Object> entity = new HashMap<>();
        entity.putAll(map);
        Map<String, Object> metadata = (Map<String, Object>) entity.remove(Constants.Documents.Metadata.KEY);
        List<Document> documents = new ArrayList<>(Documents.of(entity));
        documents.add(Document.of(ID_FIELD, metadata.get(Constants.Documents.Metadata.ID)));
        return DocumentEntity.of(metadata.get(Constants.Documents.Metadata.COLLECTION).toString(), documents);
    }

    static Map<String, Object> getMap(DocumentEntity entity) {

        Map<String, Object> entityMap = new HashMap<>();

        entity.getDocuments().stream()
                .filter(d -> !ID_FIELD.equals(d.getName()))
                .forEach(feedJSON(entityMap));
        return entityMap;
    }


    private static Consumer<Document> feedJSON(Map<String, Object> map) {
        return d -> {
            Object value = ValueUtil.convert(d.getValue());
            if (value instanceof Document) {
                Document subDocument = Document.class.cast(value);
                map.put(d.getName(), singletonMap(subDocument.getName(), subDocument.get()));
            } else if (isSudDocument(value)) {
                Map<String, Object> subDocument = getMap(value);
                map.put(d.getName(), subDocument);
            } else if (isSudDocumentList(value)) {
                map.put(d.getName(), StreamSupport.stream(Iterable.class.cast(value).spliterator(), false)
                        .map(EntityConverter::getMap).collect(toList()));
            } else {
                map.put(d.getName(), value);
            }
        };
    }

    private static Map<String, Object> getMap(Object value) {
        Map<String, Object> subDocument = new HashMap<>();
        StreamSupport.stream(Iterable.class.cast(value).spliterator(),
                false).forEach(feedJSON(subDocument));
        return subDocument;
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(org.jnosql.diana.api.document.Document.class::isInstance);
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }


}
