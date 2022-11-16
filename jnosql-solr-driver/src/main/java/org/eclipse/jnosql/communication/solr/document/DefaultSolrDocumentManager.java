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
 */

package org.eclipse.jnosql.communication.solr.document;

import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * The default implementation of {@link SolrDocumentManager}
 */
class DefaultSolrDocumentManager implements SolrDocumentManager {

    private final HttpSolrClient solrClient;

    private final String database;

    private final boolean automaticCommit;

    DefaultSolrDocumentManager(HttpSolrClient solrClient, String database, boolean automaticCommit) {
        this.solrClient = solrClient;
        this.database = database;
        this.automaticCommit = automaticCommit;
    }


    @Override
    public String getName() {
        return database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");

        try {
            solrClient.add(SolrUtils.getDocument(entity));
            commit();
        } catch (SolrServerException | IOException e) {
            throw new SolrException("Error to insert/update a information", e);
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
        final List<SolrInputDocument> documents = StreamSupport.stream(entities.spliterator(), false)
                .map(SolrUtils::getDocument).collect(toList());
        try {
            solrClient.add(documents);
            commit();
        } catch (SolrServerException | IOException e) {
            throw new SolrException("Error to insert/update a information", e);
        }
        return entities;
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
        try {
            solrClient.deleteByQuery(DocumentQueryConversor.convert(query));
            commit();
        } catch (SolrServerException | IOException e) {
            throw new SolrException("Error to delete at Solr", e);
        }
    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required");
        try {
            SolrQuery solrQuery = new SolrQuery();
            final String queryExpression = DocumentQueryConversor.convert(query);
            solrQuery.set("q", queryExpression);
            if (query.getSkip() > 0) {
                solrQuery.setStart((int) query.getSkip());
            }
            if (query.getLimit() > 0) {
                solrQuery.setRows((int) query.getLimit());
            }
            final List<SortClause> sorts = query.getSorts().stream()
                    .map(s -> new SortClause(s.getName(), s.getType().name().toLowerCase(Locale.US)))
                    .collect(toList());
            solrQuery.setSorts(sorts);
            final QueryResponse response = solrClient.query(solrQuery);
            final SolrDocumentList documents = response.getResults();
            return SolrUtils.of(documents).stream();
        } catch (SolrServerException | IOException e) {
            throw new SolrException("Error to query at Solr", e);
        }
    }

    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "documentCollection is required");

        try {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.set("q", "_entity:" + documentCollection);
            solrQuery.setRows(0);
            final QueryResponse response = solrClient.query(solrQuery);
            return response.getResults().getNumFound();
        } catch (SolrServerException | IOException e) {
            throw new SolrException("Error to execute count at Solr", e);
        }
    }

    @Override
    public void close() {

    }

    private void commit() {
        if (isAutomaticCommit()) {
            try {
                solrClient.commit();
            } catch (SolrServerException | IOException e) {
                throw new SolrException("Error to commit at Solr", e);
            }
        }
    }

    private Boolean isAutomaticCommit() {
        return automaticCommit;
    }


    @Override
    public List<DocumentEntity> solr(String query) {
        Objects.requireNonNull(query, "query is required");

        try {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.set("q", query);
            final QueryResponse response = solrClient.query(solrQuery);
            final SolrDocumentList documents = response.getResults();
            return SolrUtils.of(documents);
        } catch (SolrServerException | IOException e) {
            throw new SolrException("Error to execute native query at Solr query: " + query, e);
        }
    }

    @Override
    public List<DocumentEntity> solr(String query, Map<String, ? extends Object> params) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(params, "params is required");
        String nativeQuery = query;
        for (Entry<String, ? extends Object> entry : params.entrySet()) {
            nativeQuery = nativeQuery.replace('@' + entry.getKey(), entry.getValue().toString());
        }
        return solr(nativeQuery);
    }
}
