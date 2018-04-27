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
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.documents.session.IMetadataDictionary;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static net.ravendb.client.Constants.Documents.Metadata.COLLECTION;
import static net.ravendb.client.Constants.Documents.Metadata.ID;
import static org.jnosql.diana.ravendb.document.EntityConverter.ID_FIELD;

/**
 * The mongodb implementation to {@link DocumentCollectionManager} that does not support TTL methods
 * <p>{@link RavenDBDocumentCollectionManager#insert(DocumentEntity, Duration)}</p>
 */
public class RavenDBDocumentCollectionManager implements DocumentCollectionManager {


    private final DocumentStore documentStore;


    RavenDBDocumentCollectionManager(DocumentStore documentStore) {
        this.documentStore = documentStore;
        documentStore.getConventions().registerIdConvention(Map.class, (dbName, map) -> {

            return (String) map.get("collection") + "/" + map.getOrDefault(EntityConverter.ID_FIELD, "");
        });

    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {

        Objects.requireNonNull(entity, "entity is required");



        try (IDocumentSession session = documentStore.openSession()) {
            IMetadataDictionary metadata = session.advanced().getMetadataFor(EntityConverter.getMap(entity));
            Optional<Document> id = entity.find(ID_FIELD);
            String collection = entity.getName();
            metadata.put(COLLECTION, collection);
            id.ifPresent(i -> metadata.put(ID, collection + "/" + i.get(String.class)));
        }
        return entity;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support save with TTL");
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return null;
    }


    @Override
    public void delete(DocumentDeleteQuery query) {
    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        return null;
    }

    @Override
    public void close() {
        documentStore.close();
    }


}
