/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

final class OracleDocumentManager implements DocumentManager {
    private final String table;
    private final NoSQLHandle serviceHandle;

    public OracleDocumentManager(String table, NoSQLHandle serviceHandle) {
        this.table = table;
        this.serviceHandle = serviceHandle;
    }

    @Override
    public String name() {
        return table;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");

        return null;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        return null;
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        return null;
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        return null;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return null;
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        return null;
    }

    @Override
    public void delete(DocumentDeleteQuery query) {

    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) {
        return null;
    }

    @Override
    public long count(String documentCollection) {
        return 0;
    }

    @Override
    public void close() {

    }
}
