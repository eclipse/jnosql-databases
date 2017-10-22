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
package org.jnosql.diana.mongodb.document;

import org.bson.Document;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.util.function.Function;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;

final class MongoDBUtils {
    private static final ValueWriter WRITER = ValueWriterDecorator.getInstance();
    private static final Function<Object, String> KEY_DOCUMENT = d -> cast(d).getName();
    private static final Function<Object, Object> VALUE_DOCUMENT = d -> cast(d).get();

    private MongoDBUtils() {
    }

    static Document getDocument(DocumentEntity entity) {
        Document document = new Document();
        entity.getDocuments().stream().forEach(d -> document.append(d.getName(), convert(d.getValue())));
        return document;
    }

    private static Object convert(Value value) {
        Object val = value.get();
        if (val instanceof org.jnosql.diana.api.document.Document) {
            org.jnosql.diana.api.document.Document subDocument = (org.jnosql.diana.api.document.Document) val;
            Object converted = convert(subDocument.getValue());
            return new Document(subDocument.getName(), converted);
        }
        if (isSudDocument(val)) {
            return StreamSupport.stream(Iterable.class.cast(val).spliterator(), false)
                    .collect(toMap(KEY_DOCUMENT, VALUE_DOCUMENT));
        }
        if (WRITER.isCompatible(val.getClass())) {
            return WRITER.write(val);
        }
        return val;
    }

    private static org.jnosql.diana.api.document.Document cast(Object document) {
        return org.jnosql.diana.api.document.Document.class.cast(document);
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> org.jnosql.diana.api.document.Document.class.isInstance(d));
    }
}
