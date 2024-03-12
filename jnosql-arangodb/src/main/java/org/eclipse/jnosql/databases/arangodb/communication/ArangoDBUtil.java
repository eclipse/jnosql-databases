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
package org.eclipse.jnosql.databases.arangodb.communication;


import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import org.eclipse.jnosql.communication.Value;

import org.eclipse.jnosql.communication.ValueUtil;
import org.eclipse.jnosql.communication.semistructured.CommunicationEntity;
import org.eclipse.jnosql.communication.semistructured.Element;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    private static final Logger LOGGER = Logger.getLogger(ArangoDBUtil.class.getName());

    private static final Function<Map.Entry<?, ?>, Element> ENTRY_DOCUMENT = entry ->
            Element.of(entry.getKey().toString(), entry.getValue());

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


    static CommunicationEntity toEntity(BaseDocument document) {
        Map<String, Object> properties = document.getProperties();
        List<Element> documents = properties.keySet().stream()
                .map(k -> toDocument(k, properties))
                .collect(Collectors.toList());

        documents.add(Element.of(KEY, document.getKey()));
        documents.add(Element.of(ID, document.getId()));
        documents.add(Element.of(REV, document.getRevision()));
        String collection = document.getId().split("/")[0];
        return CommunicationEntity.of(collection, documents);
    }

    static BaseDocument getBaseDocument(CommunicationEntity entity) {
        Map<String, Object> map = new HashMap<>();
        for (Element document : entity.elements()) {
            if(KEY.equals(document.name()) && Objects.isNull(document.get())) {
                continue;
            }
            map.put(document.name(), convert(document.value()));
        }
        return new BaseDocument(map);
    }

    private static Element toDocument(String key, Map<String, Object> properties) {
        Object value = properties.get(key);
        if (value instanceof Map map) {
            return Element.of(key, map.keySet()
                    .stream().map(k -> toDocument(k.toString(), map))
                    .collect(Collectors.toList()));
        }
        if (isADocumentIterable(value)) {
            List<List<Element>> documents = new ArrayList<>();
            for (Object object : Iterable.class.cast(value)) {
                Map<?, ?> map = Map.class.cast(object);
                documents.add(map.entrySet().stream().map(ENTRY_DOCUMENT).collect(toList()));
            }
            return Element.of(key, documents);

        }
        return Element.of(key, value);
    }

    private static boolean isADocumentIterable(Object value) {
        return Iterable.class.isInstance(value) &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(Map.class::isInstance);
    }

    private static Object convert(Value value) {
        Object val = ValueUtil.convert(value);

        if (Element.class.isInstance(val)) {
            Element document = Element.class.cast(val);
            return singletonMap(document.name(), convert(document.value()));
        }
        if (isSudDocument(val)) {
            return getMap(val);
        }
        if (isSudDocumentList(val)) {
            return StreamSupport.stream(Iterable.class.cast(val).spliterator(), false)
                    .map(ArangoDBUtil::getMap).collect(toList());
        }
        return val;
    }

    private static Object getMap(Object val) {
        Iterable<?> iterable = Iterable.class.cast(val);
        Map<Object, Object> map = new HashMap<>();
        for (Object item : iterable) {
            var document = cast(item);
            map.put(document.name(), document.get());
        }
        return map;
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(Element.class::isInstance);
    }

    private static Element cast(Object document) {
        return Element.class.cast(document);
    }

}
