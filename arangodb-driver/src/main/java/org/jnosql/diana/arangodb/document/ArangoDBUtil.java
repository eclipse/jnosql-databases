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
package org.jnosql.diana.arangodb.document;


import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * The utilitarian class to ArangoDB
 */
public final class ArangoDBUtil {

    public static final String KEY = "_key";
    public static final String ID = "_id";
    public static final String REV = "_rev";

    private static final ValueWriter WRITER = ValueWriterDecorator.getInstance();


    private static final Logger LOGGER = Logger.getLogger(ArangoDBUtil.class.getName());

    private ArangoDBUtil() {
    }


    static void checkDatabase(String database, ArangoDB arangoDB) {
        Objects.requireNonNull(database, "database is required");
        try {
            Collection<String> databases = arangoDB.getDatabases();
            if (!databases.contains(database)) {
                arangoDB.createDatabase(database);
            }
        } catch (ArangoDBException e) {
            LOGGER.log(Level.WARNING, "Failed to create database: " + database, e);
        }
    }

    public static void checkCollection(String bucketName, ArangoDB arangoDB, String namespace) {
        checkDatabase(bucketName, arangoDB);
        List<String> collections = arangoDB.db(bucketName)
                .getCollections().stream()
                .map(CollectionEntity::getName)
                .collect(toList());
        if (!collections.contains(namespace)) {
            arangoDB.db(bucketName).createCollection(namespace);
        }
    }


    public static boolean checkCondition(Optional<DocumentCondition> query) {
        return query.isPresent();
    }

    static DocumentEntity toEntity(String collection, BaseDocument document) {
        Map<String, Object> properties = document.getProperties();
        List<Document> documents = properties.keySet().stream()
                .map(k -> toDocument(k, properties))
                .collect(Collectors.toList());
        documents.add(Document.of(KEY, document.getKey()));
        documents.add(Document.of(ID, document.getId()));
        documents.add(Document.of(REV, document.getRevision()));
        return DocumentEntity.of(collection, documents);
    }

    static BaseDocument getBaseDocument(DocumentEntity entity) {
        Map<String, Object> map = new HashMap<>();
        for (Document document : entity.getDocuments()) {
            map.put(document.getName(), convert(document.getValue()));
        }
        return new BaseDocument(map);
    }

    private static Document toDocument(String key, Map<String, Object> properties) {
        Object value = properties.get(key);
        if (Map.class.isInstance(value)) {
            Map map = Map.class.cast(value);
            return Document.of(key, map.keySet()
                    .stream().map(k -> toDocument(k.toString(), map))
                    .collect(Collectors.toList()));
        }
        if (isADocumentIterable(value)) {
            List<Document> documents = new ArrayList<>();
            for (Object object : Iterable.class.cast(value)) {
                documents.add(Document.of(Map.class.cast(object).get("name").toString(),
                        Map.class.cast(Map.class.cast(object).get("value")).get("value")));
            }
            return Document.of(key, documents);

        }
        return Document.of(key, value);
    }

    private static boolean isADocumentIterable(Object value) {
        return Iterable.class.isInstance(value) &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(d -> Map.class.isInstance(d));
    }

    private static Object convert(Value value) {
        Object val = value.get();
        if (WRITER.isCompatible(val.getClass())) {
            return WRITER.write(val);
        }
        if (Document.class.isInstance(val)) {
            Document document = Document.class.cast(val);
            return singletonMap(document.getName(), convert(document.getValue()));
        }
        return val;
    }

}
