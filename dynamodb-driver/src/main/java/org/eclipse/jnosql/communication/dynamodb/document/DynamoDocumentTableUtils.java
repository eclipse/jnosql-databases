/*
 *  Copyright (c) 2022 Otávio Santana and others
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
 *   Alessandro Moscatelli
 */
package org.eclipse.jnosql.communication.dynamodb.document;

import jakarta.nosql.document.DocumentEntity;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import static org.eclipse.jnosql.communication.dynamodb.DynamoTableUtils.createAttributeDefinition;
import static org.eclipse.jnosql.communication.dynamodb.DynamoTableUtils.createKeyElementSchema;
import static org.eclipse.jnosql.communication.dynamodb.DynamoTableUtils.createProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public final class DynamoDocumentTableUtils {

    static final Collection<Class> nTypes = Arrays.asList(Number.class, Boolean.class);
    static final Collection<Class> sTypes = Arrays.asList(Date.class, String.class);
    static final String ID_FIELD = "_id";

    private DynamoDocumentTableUtils() {
    }

    public static <T> ScalarAttributeType getScalarAttributeType(T value) {
        if (nTypes.stream().anyMatch(type -> type.isInstance(value))) {
            return ScalarAttributeType.N;
        } else if (sTypes.stream().anyMatch(type -> type.isInstance(value))) {
            return ScalarAttributeType.S;
        }
        return ScalarAttributeType.UNKNOWN_TO_SDK_VERSION;
    }

    public static Map<String, KeyType> createKeyDefinition(DocumentEntity documentEntity) {
        return Collections.singletonMap(ID_FIELD, KeyType.HASH);
    }

    public static Map<String, ScalarAttributeType> createAttributesType(DocumentEntity documentEntity) {
        return Map.ofEntries(
                documentEntity.getDocuments().stream().map(
                        document -> new AbstractMap.SimpleEntry<>(
                                document.getName(),
                                getScalarAttributeType(document.getValue().get())
                        )
                ).toArray(
                        AbstractMap.SimpleEntry[]::new
                )
        );
    }

    public static void createTable(String tableName, DynamoDbClient client, DocumentEntity documentEntity, Long readCapacityUnits, Long writeCapacityUnit) {

        client.createTable(
                CreateTableRequest.builder()
                        .tableName(tableName)
                        .provisionedThroughput(
                                createProvisionedThroughput(readCapacityUnits, writeCapacityUnit)
                        )
                        .keySchema(
                                createKeyElementSchema(
                                        Collections.singletonMap(
                                                ID_FIELD,
                                                KeyType.HASH
                                        )
                                )
                        )
                        .attributeDefinitions(
                                createAttributeDefinition(
                                        Collections.singletonMap(
                                                ID_FIELD,
                                                getScalarAttributeType(
                                                        documentEntity.find(ID_FIELD).get().getValue().get()
                                                )
                                        )
                                )
                        )
                        .build()
        );

        client.waiter().waitUntilTableExists(t -> t.tableName(tableName));

    }

}
