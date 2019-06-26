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

package org.jnosql.diana.solr.document;

import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.jnosql.diana.SettingsPriority;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.solr.document.SolrUtils.getDocument;

/**
 * The solr implementation to {@link DocumentCollectionManager} that does not support TTL methods
 * <p>{@link SolrBDocumentCollectionManager#insert(DocumentEntity, Duration)}</p>
 */
public class SolrBDocumentCollectionManager implements DocumentCollectionManager {

    private final HttpSolrClient solrClient;

    SolrBDocumentCollectionManager(HttpSolrClient solrClient) {
        this.solrClient = solrClient;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");

        final SolrInputDocument document = getDocument(entity);
        try {
            solrClient.add(document);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        if (isAutomaticCommit()) {
            commit();
        }
        return entity;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("Apache Solr does not support save with TTL");
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
        Objects.requireNonNull(query, "query is required");


    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required");
        return Collections.emptyList();

    }

    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "documentCollection is required");
        return 0L;
    }

    @Override
    public void close() {

    }

    private void commit() {
        try {
            solrClient.commit();
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
        }

    }

    private Boolean isAutomaticCommit() {
        return SettingsPriority.get("jakarta.nosql.transaction")
                .map(Object::toString)
                .map(Boolean::parseBoolean).orElse(true);
    }


}
