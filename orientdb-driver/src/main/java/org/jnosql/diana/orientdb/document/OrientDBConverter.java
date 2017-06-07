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
