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

import jakarta.nosql.document.DocumentCollectionManagerFactory;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.Objects;

/**
 * The solr implementation to {@link DocumentCollectionManagerFactory}
 */
public class SolrDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory {

    private final HttpSolrClient solrClient;

    SolrDocumentCollectionManagerFactory(HttpSolrClient solrClient) {
        this.solrClient = solrClient;
    }

    @Override
    public SolrBDocumentCollectionManager get(String database) {
        Objects.requireNonNull(database, "database is required");
        final String baseURL = solrClient.getBaseURL() + '/' + database;
        return new DefaultSolrBDocumentCollectionManager(new HttpSolrClient.Builder(baseURL).build());
    }


    @Override
    public void close() {
    }
}
