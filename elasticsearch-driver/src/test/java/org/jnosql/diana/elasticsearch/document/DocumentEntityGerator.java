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
package org.jnosql.diana.elasticsearch.document;

import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentEntity;
import org.jnosql.diana.document.Documents;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class DocumentEntityGerator {

    static final String COLLECTION_NAME = "person";
    static final String INDEX = "person";

    static DocumentEntity getEntity() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Poliana");
        map.put("city", "Salvador");
        map.put("_id", "id");
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }
}
