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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import org.apache.commons.collections.map.HashedMap;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;

class DefaultOrientDBDocumentCollectionManager implements OrientDBDocumentCollectionManager {


    private static final Consumer<DocumentEntity> NOOPS = d -> {
    };
    private final OPartitionedDatabasePool pool;

    DefaultOrientDBDocumentCollectionManager(OPartitionedDatabasePool pool) {
        this.pool = pool;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
        Objects.toString(entity, "Entity is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            ODocument document = new ODocument(entity.getName());

            Map<String, Object> entityValues = toMap(entity);
            entityValues.keySet().stream().forEach(k -> document.field(k, entityValues.get(k)));
            ODocument save = null;
            try {
                save = tx.save(document);
            } catch (ONeedRetryException e) {
                save = tx.reload(document);
            }
            if (Objects.nonNull(save)) {
                ORecordId ridField = save.field("@rid");
                if (Objects.nonNull(ridField)) {
                    entity.add(Document.of(RID_FIELD, ridField.toString()));
                }
            }

            return entity;
        }
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("There is support to ttl on OrientDB");
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return insert(entity);
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            OSQLQueryFactory.QueryResult orientQuery = OSQLQueryFactory
                    .to(DocumentQuery.of(query.getCollection())
                            .and(query.getCondition()
                                    .orElseThrow(() -> new IllegalArgumentException("Condition is required"))));

            List<ODocument> result = tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
            result.forEach(tx::delete);
        }

    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            OSQLQueryFactory.QueryResult orientQuery = OSQLQueryFactory.to(query);
            List<ODocument> result = tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
            return OrientDBConverter.convert(result);
        }
    }

    /**
     * Find using query
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    public List<DocumentEntity> select(String query, Object... params) throws NullPointerException {
        requireNonNull(query, "query is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            List<ODocument> result = tx.command(OSQLQueryFactory.parse(query)).execute(params);
            return OrientDBConverter.convert(result);
        }

    }

    @Override
    public void live(DocumentQuery query, Consumer<DocumentEntity> callBack) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callback is required");
        try (ODatabaseDocumentTx tx = pool.acquire();) {
            OSQLQueryFactory.QueryResult queryResult = OSQLQueryFactory.toLive(query, callBack);
            tx.command(queryResult.getQuery()).execute(queryResult.getParams());
        }
    }

    @Override
    public void live(String query, Consumer<DocumentEntity> callBack, Object... params) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callback is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            OLiveQuery<ODocument> liveQuery = new OLiveQuery<>(query, new LiveQueryLIstener(callBack));
            tx.command(liveQuery).execute(params);
        }

    }

    @Override
    public void close() {
        pool.close();
    }

    private Map<String, Object> toMap(DocumentEntity entity) {
        Map<String, Object> entityValues = new HashedMap();
        for (Document document : entity.getDocuments()) {
            toDocument(entityValues, document);

        }

        return entityValues;
    }

    private void toDocument(Map<String, Object> entityValues, Document document) {
        Object valueAsObject = ValueUtil.convert(document.getValue());
        if (Document.class.isInstance(valueAsObject)) {
            Document subDocument = Document.class.cast(valueAsObject);
            entityValues.put(document.getName(), singletonMap(subDocument.getName(), subDocument.get()));
        } else if (isADocumentIterable(valueAsObject)) {
            Map<String, Object> map = new java.util.HashMap<>();
            stream(Iterable.class.cast(valueAsObject).spliterator(), false)
                    .forEach(d ->  toDocument(map, Document.class.cast(d)));
            entityValues.put(document.getName(), map);
        } else {
            entityValues.put(document.getName(), document.get());
        }
    }

    private static boolean isADocumentIterable(Object value) {
        return Iterable.class.isInstance(value) &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(d -> Document.class.isInstance(d));
    }
}
