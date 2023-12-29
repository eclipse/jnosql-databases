/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.communication;

import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentPreparedStatement;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

public class DynamoDBDocumentManager implements DocumentManager {

    private final String database;
    private final DynamoDbClient dynamoDbClient;

    public DynamoDBDocumentManager(String database, DynamoDbClient dynamoDbClient) {
        this.database = database;
        this.dynamoDbClient = dynamoDbClient;
    }

    public DynamoDbClient getDynamoDbClient() {
        return dynamoDbClient;
    }

    @Override
    public String name() {
        return database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity documentEntity) {
        return null;
    }

    @Override
    public DocumentEntity insert(DocumentEntity documentEntity, Duration duration) {
        return null;
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> iterable) {
        return null;
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> iterable, Duration duration) {
        return null;
    }

    @Override
    public DocumentEntity update(DocumentEntity documentEntity) {
        return null;
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> iterable) {
        return null;
    }

    @Override
    public void delete(DocumentDeleteQuery documentDeleteQuery) {

    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery documentQuery) {
        return null;
    }

    @Override
    public long count(DocumentQuery query) {
        return DocumentManager.super.count(query);
    }

    @Override
    public boolean exists(DocumentQuery query) {
        return DocumentManager.super.exists(query);
    }

    @Override
    public Stream<DocumentEntity> query(String query) {
        return DocumentManager.super.query(query);
    }

    @Override
    public DocumentPreparedStatement prepare(String query) {
        return DocumentManager.super.prepare(query);
    }

    @Override
    public Optional<DocumentEntity> singleResult(DocumentQuery query) {
        return DocumentManager.super.singleResult(query);
    }

    @Override
    public long count(String s) {
        return 0;
    }

    @Override
    public void close() {
        this.dynamoDbClient.close();
    }
}
