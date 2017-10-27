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
package org.jnosql.diana.arangodb.document;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import com.arangodb.entity.BaseDocument;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.api.Condition.EQUALS;
import static org.jnosql.diana.api.Condition.IN;

final class OperationsByKeysUtils {

    private OperationsByKeysUtils() {
    }

    public static List<DocumentEntity> findByKeys(DocumentQuery query, ArangoDB arangoDB, String database) {
        DocumentCondition condition = query.getCondition().get();
        Value value = condition.getDocument().getValue();
        String collection = query.getDocumentCollection();
        if (EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            DocumentEntity entity = toEntity(collection, key, arangoDB, database);
            if (Objects.isNull(entity)) {
                return Collections.emptyList();
            }
            return singletonList(entity);
        } else if (IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            return keys.stream().map(k -> toEntity(collection, k, arangoDB, database))
                    .collect(toList());
        }

        return Collections.emptyList();
    }

    public static void findByKeys(DocumentQuery query, Consumer<List<DocumentEntity>> callBack,
                                  ArangoDBAsync arangoDBAsync, String database) {

        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        Value value = condition.getDocument().getValue();
        String collection = query.getDocumentCollection();
        if (Condition.EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            CompletableFuture<BaseDocument> future = arangoDBAsync.db(database).collection(collection)
                    .getDocument(key, BaseDocument.class);

            future.thenAccept(d -> callBack.accept(singletonList(ArangoDBUtil.toEntity(d))));

            return;
        }
        if (Condition.IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            List<DocumentEntity> entities = synchronizedList(new ArrayList<>());

            if (keys.isEmpty()) {
                callBack.accept(Collections.emptyList());
                return;
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String key : keys) {
                CompletableFuture<BaseDocument> future = arangoDBAsync.db(database).collection(collection)
                        .getDocument(key, BaseDocument.class);
                futures.add(future.thenAcceptAsync(d -> entities.add(ArangoDBUtil.toEntity(d))));
            }
            CompletableFuture.allOf(futures.toArray(
                    new CompletableFuture[futures.size()]))
                    .thenAcceptAsync(v -> callBack.accept(entities));

        }

        callBack.accept(Collections.emptyList());
    }

    private static DocumentEntity toEntity(String collection, String key, ArangoDB arangoDB, String database) {
        BaseDocument document = arangoDB.db(database).collection(collection).getDocument(key, BaseDocument.class);
        if (Objects.isNull(document)) {
            return null;
        }
        return ArangoDBUtil.toEntity(document);
    }

    public static boolean isJustKey(Optional<DocumentCondition> documentCondition, String key) {
        if (documentCondition.isPresent()) {
            DocumentCondition condition = documentCondition.get();
            boolean isKeyDocument = key.equals(condition.getDocument().getName());
            boolean isEqualsKeys = EQUALS.equals(condition.getCondition()) && isKeyDocument;
            boolean isINKeys = IN.equals(condition.getCondition()) && isKeyDocument;
            return isEqualsKeys || isINKeys;
        }
        return false;
    }

    public static void deleteByKey(DocumentDeleteQuery query, String collection, ArangoDB arangoDB, String database) {
        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        Value value = condition.getDocument().getValue();
        if (IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            arangoDB.db(database).collection(collection).deleteDocuments(keys);
        } else if (EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            arangoDB.db(database).collection(collection).deleteDocument(key);
        }
    }
}
