/*
 * Copyright 2017 Otavio Santana and others
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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.record.impl.ODocument;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

final class OrientDBConverter {

    static final String RID_FIELD = "@rid";

    private OrientDBConverter() {
    }

    static List<DocumentEntity> convert(List<ODocument> result) {
        List<DocumentEntity> entities = new ArrayList<>();
        for (ODocument document : result) {

            DocumentEntity entity = convert(document);
            entities.add(entity);
        }
        return entities;
    }

    static DocumentEntity convert(ODocument document) {
        DocumentEntity entity = DocumentEntity.of(document.getClassName());
        Stream.of(document.fieldNames())
                .map(f -> Document.of(f, (Object) document.field(f)))
                .forEach(entity::add);
        entity.add(Document.of(RID_FIELD, document.field(RID_FIELD).toString()));
        return entity;
    }
}
