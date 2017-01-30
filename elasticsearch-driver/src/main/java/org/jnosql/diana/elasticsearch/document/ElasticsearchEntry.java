/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.elasticsearch.document;


import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.Documents;

import java.util.Map;

import static java.util.Objects.isNull;
import static org.jnosql.diana.elasticsearch.document.ElasticsearchDocumentCollectionManager.ID_FIELD;

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
        DocumentEntity entity = DocumentEntity.of(collection, Documents.of(map));
        entity.remove(ID_FIELD);
        entity.add(id);
        return entity;
    }

}
