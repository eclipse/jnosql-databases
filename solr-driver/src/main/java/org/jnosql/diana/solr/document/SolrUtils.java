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
package org.jnosql.diana.solr.document;

import jakarta.nosql.Value;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentEntity;
import org.apache.solr.common.SolrInputDocument;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

final class SolrUtils {
    static final String ID_FIELD = "_id";

    private static final Function<Object, String> KEY_DOCUMENT = d -> cast(d).getName();
    private static final Function<Object, Object> VALUE_DOCUMENT = d -> SolrUtils.convert(cast(d).getValue());

    private SolrUtils() {
    }

    static SolrInputDocument getDocument(DocumentEntity entity) {
        SolrInputDocument document = new SolrInputDocument();
        entity.getDocuments().stream().forEach(d -> document.addField(d.getName(), convert(d.getValue())));
        return document;
    }

    private static Object convert(Value value) {
        Object val = ValueUtil.convert(value);
        if (val instanceof Document) {
            jakarta.nosql.document.Document subDocument = (jakarta.nosql.document.Document) val;
            Object converted = convert(subDocument.getValue());
            return Collections.singletonMap(subDocument.getName(), converted);
        }
        if (isSudDocument(val)) {
            return getMap(val);
        }
        if (isSudDocumentList(val)) {
            return StreamSupport.stream(Iterable.class.cast(val).spliterator(), false)
                    .map(SolrUtils::getMap).collect(toList());
        }
        return val;
    }


    public static List<jakarta.nosql.document.Document> of(Map<String, ?> values) {
        Predicate<String> isNotNull = s -> values.get(s) != null;
        Function<String, jakarta.nosql.document.Document> documentMap = key -> {
            Object value = values.get(key);
            return getDocument(key, value);
        };
        return values.keySet().stream().filter(isNotNull).map(documentMap).collect(Collectors.toList());
    }

    private static Document getDocument(String key, Object value) {
        if (value instanceof Document) {
            //return Document.of(key, of(Document.class.cast(value)));
        } else if (isDocumentIterable(value)) {
            List<List<jakarta.nosql.document.Document>> documents = new ArrayList<>();
            for (Object object : Iterable.class.cast(value)) {
                Map<?, ?> map = Map.class.cast(object);
                documents.add(map.entrySet().stream().map(e -> getDocument(e.getKey().toString(), e.getValue())).collect(toList()));
            }
            return jakarta.nosql.document.Document.of(key, documents);
        }

        return Document.of(key, Value.of(value));
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

    private static jakarta.nosql.document.Document cast(Object document) {
        return jakarta.nosql.document.Document.class.cast(document);
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(jakarta.nosql.document.Document.class::isInstance);
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }
}
