/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

class DefaultCouchDBDocumentCollectionManager implements CouchDBDocumentCollectionManager {


    private final CouchDBHttpClient connector;

    DefaultCouchDBDocumentCollectionManager(CouchDBHttpClient connector) {
        this.connector = connector;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        return connector.insert(entity);
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        return connector.update(entity);
    }


    @Override
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        connector.delete(query);
    }

    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required");
        return connector.select(query);
    }

    @Override
    public long count() {
        return connector.count();
    }


    @Override
    public long count(String documentCollection) {
       throw new UnsupportedOperationException("CouchDB does not have support to count by document Collection," +
               " to total of elments at database use CouchDBDocumentCollectionManager#count");
    }


    @Override
    public void close() {
        connector.close();
    }


}
