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
import com.arangodb.entity.DocumentDeleteEntity;
import com.arangodb.entity.MultiDocumentEntity;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.jnosql.diana.api.Condition.EQUALS;
import static org.jnosql.diana.api.Condition.IN;

final class OperationsByKeysUtils {

    private OperationsByKeysUtils() {
    }



    private static DocumentEntity toEntity(String collection, String key, ArangoDB arangoDB, String database) {
        BaseDocument document = arangoDB.db(database).collection(collection).getDocument(key, BaseDocument.class);
        if (Objects.isNull(document)) {
            return null;
        }
        return ArangoDBUtil.toEntity(document);
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

    public static void deleteByKey(DocumentDeleteQuery query, Consumer<Void> callBack,
                                   ArangoDBAsync arangoDBAsync, String database) {

        String collection = query.getDocumentCollection();
        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        Value value = condition.getDocument().getValue();
        if (IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            CompletableFuture<MultiDocumentEntity<DocumentDeleteEntity<Void>>> future = arangoDBAsync.db(database)
                    .collection(collection).deleteDocuments(keys);
            future.thenAccept(d -> callBack.accept(null));
        } else if (EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            CompletableFuture<DocumentDeleteEntity<Void>> future = arangoDBAsync.db(database).
                    collection(collection).deleteDocument(key);
            future.thenAccept(d -> callBack.accept(null));
        }
    }
}
