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
package org.eclipse.jnosql.communication.solr.document;

import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.Documents;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final class SolrUtils {

    static final String ENTITY = "_entity";

    private SolrUtils() {
    }

    static SolrInputDocument getDocument(DocumentEntity entity) {
        SolrInputDocument document = new SolrInputDocument();
        document.addField(ENTITY, entity.getName());
        entity.documents().stream().forEach(d -> document.addField(d.getName(), convert(d.getValue())));
        return document;
    }

    private static Object convert(Value value) {
        Object val = ValueUtil.convert(value);
        if (val instanceof Document || isSudDocument(val) || isSudDocumentList(val)) {
            throw new SolrException("Apache Solr does not support to embedded field");
        }
        return val;
    }


    public static List<DocumentEntity> of(SolrDocumentList values) {

        return values.stream()
                .map(SolrDocument::getFieldValueMap)
                .map(SolrUtils::solrToMap)
                .map(Documents::of)
                .map(documents -> {
                    final String entity = documents.stream()
                            .filter(d -> ENTITY.equals(d.getName()))
                            .findFirst()
                            .map(Document::getName)
                            .orElseThrow(() -> new SolrException("The field _entity is required"));
                    return DocumentEntity.of(entity, documents);
                }).collect(Collectors.toList());
    }

    private static Map<String, Object> solrToMap(Map<String, Object> map) {
        return map.keySet().stream().collect(Collectors.toMap(k -> k, map::get));
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
