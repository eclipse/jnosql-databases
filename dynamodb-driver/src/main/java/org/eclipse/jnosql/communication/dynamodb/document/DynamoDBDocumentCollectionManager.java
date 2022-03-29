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

import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import java.time.Duration;
import java.util.stream.Stream;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class DynamoDBDocumentCollectionManager implements DocumentCollectionManager {

    private final DynamoDbClient client;
    private final String tableName;
    private Boolean initializeTable;

    public DynamoDBDocumentCollectionManager(
            DynamoDbClient client,
            String tableName,
            Boolean initializeTable
    ) {
        this.client = client;
        this.tableName = tableName;
        this.initializeTable = initializeTable;
    }
    
    private void checkTable(DocumentEntity de) {
        if (initializeTable) {
            DynamoDocumentTableUtils.createTable(tableName, client, de, null, null);
            initializeTable = Boolean.FALSE;
        }
    }

    @Override
    public DocumentEntity insert(DocumentEntity de) {
        checkTable(de);
        client.putItem(PutItemRequest.builder().tableName(tableName).item(DynamoDocumentTableUtils.createAttributesMap(de)).build());
        return de;
    }

    @Override
    public DocumentEntity insert(DocumentEntity de, Duration drtn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> itrbl) {
        checkTable(itrbl.iterator().next());
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> itrbl, Duration drtn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocumentEntity update(DocumentEntity de) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> itrbl) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void delete(DocumentDeleteQuery ddq) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery dq) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public long count(String string) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() {
        this.client.close();
    }

}
