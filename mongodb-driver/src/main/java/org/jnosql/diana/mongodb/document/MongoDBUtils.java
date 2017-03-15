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
package org.jnosql.diana.mongodb.document;

import org.bson.Document;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final class MongoDBUtils {
    private static final ValueWriter WRITER = ValueWriterDecorator.getInstance();

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
                    .map(d -> {
                        org.jnosql.diana.api.document.Document dianaDocument = org.jnosql.diana.api.document.Document.class.cast(d);
                        Document document = new Document();
                        document.append(dianaDocument.getName(), dianaDocument.get());
                        return document;
                    }).collect(Collectors.toList());
        }
        if (WRITER.isCompatible(val.getClass())) {
            return WRITER.write(val);
        }
        return val;
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> org.jnosql.diana.api.document.Document.class.isInstance(d));
    }
}
