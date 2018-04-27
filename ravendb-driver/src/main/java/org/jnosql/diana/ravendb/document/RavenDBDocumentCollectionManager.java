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

package org.jnosql.diana.ravendb.document;

import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.documents.queries.Query;
import net.ravendb.client.documents.session.IDocumentQuery;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.documents.session.IMetadataDictionary;
import net.ravendb.client.exceptions.RavenException;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.ravendb.document.DocumentQueryConversor.QueryResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.ravendb.client.Constants.Documents.Metadata.COLLECTION;

/**
 * The mongodb implementation to {@link DocumentCollectionManager} that does not support TTL methods
 * <p>{@link RavenDBDocumentCollectionManager#insert(DocumentEntity, Duration)}</p>
 */
public class RavenDBDocumentCollectionManager implements DocumentCollectionManager {


    private final DocumentStore store;


    RavenDBDocumentCollectionManager(DocumentStore store) {
        this.store = store;
        this.store.initialize();

    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        try (IDocumentSession session = store.openSession()) {
            String collection = entity.getName();

            Map<String, Object> entityMap = EntityConverter.getMap(entity);
            String id = entity.find(EntityConverter.ID_FIELD)
                    .map(d -> d.get(String.class))
                    .orElse(collection + '/');
            session.store(entityMap, id);
            IMetadataDictionary metadata = session.advanced().getMetadataFor(entityMap);
            metadata.put(COLLECTION, collection);
            session.saveChanges();
            entity.add(EntityConverter.ID_FIELD, session.advanced().getDocumentId(entityMap));
        }
        return entity;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support save with TTL");
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");

        try (IDocumentSession session = store.openSession()) {
            Document id = entity.find(EntityConverter.ID_FIELD)
                    .orElseThrow(() -> new RavenException("Id is required to Raven Update operation"));

            HashMap<String, Object> map = session.load(HashMap.class, id.get(String.class));
            map.putAll(EntityConverter.getMap(entity));
            session.saveChanges();
        }
        return entity;
    }


    @Override
    public void delete(DocumentDeleteQuery query) {
    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required");

        try (IDocumentSession session = store.openSession()) {
            List<DocumentEntity> entities = new ArrayList<>();
            QueryResult queryResult = DocumentQueryConversor.createQuery(session, query);

            List<HashMap> maps = queryResult.getIds().stream()
                    .map(i -> session.load(HashMap.class, i)).collect(Collectors.toList());

            System.out.println(maps);


        }

        return Collections.emptyList();
    }

    @Override
    public void close() {
        store.close();
    }


}
