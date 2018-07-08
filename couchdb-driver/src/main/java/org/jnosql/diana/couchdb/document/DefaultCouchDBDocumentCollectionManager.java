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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ektorp.CouchDbConnector;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

class DefaultCouchDBDocumentCollectionManager implements CouchDBDocumentCollectionManager {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENTITY = "@entity";

    private final CouchDbConnector connector;

    DefaultCouchDBDocumentCollectionManager(CouchDbConnector connector) {
        this.connector = connector;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        return save(entity, connector::create);
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return save(entity, connector::update);
    }

    private DocumentEntity save(DocumentEntity entity, Consumer<Map<String, Object>> consumer) {
        Objects.requireNonNull(entity, "entity is required");
        Map<String, Object> data = new HashMap<>(entity.toMap());
        data.put(ENTITY, entity.getName());
        consumer.accept(data);
        data.entrySet().forEach(e -> entity.add(e.getKey(), e.getValue()));
        return entity;
    }

    @Override
    public void delete(DocumentDeleteQuery query) {

    }

    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
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
