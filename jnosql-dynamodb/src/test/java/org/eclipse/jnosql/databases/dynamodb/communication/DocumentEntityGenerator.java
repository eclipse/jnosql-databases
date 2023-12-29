/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.communication;

import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.Documents;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

final class DocumentEntityGenerator {

    static final String COLLECTION_NAME = "music";


    static DocumentEntity getEntity() {
        return  getEntityWithSubDocuments(0);
    }

    static DocumentEntity getEntityWithSubDocuments(int levels) {
        Map<String, Object> map = new HashMap<>();
        map.put("_id", UUID.randomUUID().toString());
        map.put("name", "Poliana");
        map.put("hoje", LocalDate.now());
        map.put("agora", LocalDateTime.now());
        map.put("integer", 1);
        map.put("float", 1f);
        map.put("bigdecimal", BigDecimal.valueOf(10.10));
        map.put("city", "Salvador");
        map.put("texts",List.of("A","B","C"));
        if (levels > 0) {
            addSubDocument(m -> map.put("level" + levels, m), levels - 1);
        }
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }

    static void addSubDocument(Consumer<Map<String, Object>> owner, int level) {
        Map<String, Object> map = new HashMap<>();
        map.put("level", level);
        map.put("text", UUID.randomUUID().toString());
        map.put("hoje", LocalDate.now());
        map.put("agora", LocalDateTime.now());
        map.put("integer", 1);
        map.put("float", 1f);
        map.put("bigdecimal", BigDecimal.valueOf(10.10));
        if (level > 0) {
            addSubDocument(m -> map.put("level" + level, m), level - 1);
        }
        owner.accept(map);
    }
}