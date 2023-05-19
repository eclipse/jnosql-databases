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
 *   Lucas Furlaneto
 */
package org.eclipse.jnosql.databases.orientdb.communication;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.glassfish.jaxb.core.v2.model.core.ID;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.eclipse.jnosql.databases.orientdb.communication.OrientDBConverter.ID_FIELD;
import static org.eclipse.jnosql.databases.orientdb.communication.OrientDBConverter.RID_FIELD;
import static org.eclipse.jnosql.databases.orientdb.communication.OrientDBConverter.VERSION_FIELD;
import static org.eclipse.jnosql.databases.orientdb.communication.OrientDBConverter.toMap;

class DefaultOrientDBDocumentManager implements OrientDBDocumentManager {

    private final ODatabasePool pool;

    private final String database;

    DefaultOrientDBDocumentManager(ODatabasePool pool, String database) {
        this.pool = pool;
        this.database = database;
    }

    @Override
    public String name() {
        return database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        requireNonNull(entity, "Entity is required");
        try (ODatabaseSession tx = pool.acquire()) {
            ODocument document = new ODocument(entity.name());
            toMap(entity).forEach(document::field);
            try {
                tx.save(document);
            } catch (ONeedRetryException e) {
                document = tx.reload(document, OFetchHelper.DEFAULT, false);
                Map<String, Object> entityValues = toMap(entity);
                entityValues.put(OrientDBConverter.VERSION_FIELD, document.getVersion());
                entityValues.forEach(document::field);
                tx.save(document);
            }
            updateEntity(entity, document);
            return entity;
        }
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("There is no support to ttl on OrientDB");
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .collect(toList());
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> insert(e, ttl))
                .collect(toList());
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        requireNonNull(entity, "Entity is required");
        return insert(entity);
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update)
                .collect(toList());
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        requireNonNull(query, "query is required");
        DocumentQuery selectQuery = new OrientDBDocumentQuery(query);
        QueryOSQLFactory.QueryResult orientQuery = QueryOSQLFactory.to(selectQuery);

        try (ODatabaseSession tx = pool.acquire();
             OResultSet resultSet = tx.command(orientQuery.getQuery(), orientQuery.getParams())) {
            while (resultSet.hasNext()) {
                OResult next = resultSet.next();
                tx.delete(next.toElement().getIdentity());
            }
        }

    }


    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) {
        requireNonNull(query, "query is required");
        QueryOSQLFactory.QueryResult orientQuery = QueryOSQLFactory.to(query);

        try (ODatabaseSession tx = pool.acquire()) {
            List<DocumentEntity> entities = new ArrayList<>();
            if (orientQuery.isRunQuery()) {
                try (OResultSet resultSet = tx.command(orientQuery.getQuery(), orientQuery.getParams())) {
                    entities.addAll(OrientDBConverter.convert(resultSet));
                }
            }
            if (orientQuery.isLoad()) {
                orientQuery.getIds().stream().map(tx::load)
                        .map(o -> OrientDBConverter.convert((ODocument) o))
                        .forEach(entities::add);
            }
            return entities.stream();
        }
    }

    @Override
    public long count(String documentCollection) {
        requireNonNull(documentCollection, "query is required");
        try (ODatabaseSession tx = pool.acquire()) {
            String query = "select count(*) from ".concat(documentCollection);
            OResultSet command = tx.command(query);
            OResult next = command.next();
            Object count = next.getProperty("count(*)");
            return Number.class.cast(count).longValue();

        }
    }

    @Override
    public Stream<DocumentEntity> sql(String query, Object... params) {
        requireNonNull(query, "query is required");
        try (ODatabaseSession tx = pool.acquire();
             OResultSet resultSet = tx.command(query, params)) {
            return OrientDBConverter.convert(resultSet).stream();
        }

    }

    @Override
    public Stream<DocumentEntity> sql(String query, Map<String, Object> params) {
        requireNonNull(query, "query is required");
        requireNonNull(params, "params is required");

        try (ODatabaseSession tx = pool.acquire();
             OResultSet resultSet = tx.command(query, params)) {
            return OrientDBConverter.convert(resultSet).stream();
        }
    }

    @Override
    public void live(DocumentQuery query, OrientDBLiveCallback<DocumentEntity> callbacks) {
        requireNonNull(query, "query is required");
        requireNonNull(callbacks, "callbacks is required");
        ODatabaseSession tx = pool.acquire();
        QueryOSQLFactory.QueryResult queryResult = QueryOSQLFactory.toLive(query, callbacks);
        tx.live(queryResult.getQuery(), new LiveQueryListener(callbacks, tx));
    }

    @Override
    public void live(String query, OrientDBLiveCallback<DocumentEntity> callbacks, Object... params) {
        requireNonNull(query, "query is required");
        requireNonNull(callbacks, "callbacks is required");
        ODatabaseSession tx = pool.acquire();
        tx.live(query, new LiveQueryListener(callbacks, tx));

    }

    @Override
    public void close() {
        pool.close();
    }

    private void updateEntity(DocumentEntity entity, ODocument save) {
        ORecordId ridField = new ORecordId(save.getIdentity());
        entity.add(Document.of(RID_FIELD, ridField.toString()));
        entity.add(Document.of(VERSION_FIELD, save.getVersion()));
        entity.add(Document.of(ID_FIELD, ridField.toString()));
    }
}
