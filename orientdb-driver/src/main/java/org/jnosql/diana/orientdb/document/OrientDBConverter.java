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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.record.impl.ODocument;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

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
                .map(f -> Document.of(f,  convert((Object) document.field(f))))
                .forEach(entity::add);
        entity.add(Document.of(RID_FIELD, document.field(RID_FIELD).toString()));
        return entity;
    }

    private static Object convert(Object object) {
        if(Map.class.isInstance(object)) {
            Map map = Map.class.cast(object);
            return map.keySet().stream()
                    .map(k -> Document.of(k.toString(), map.get(k)))
                    .collect(toList());
        }
        return object;
    }
}
