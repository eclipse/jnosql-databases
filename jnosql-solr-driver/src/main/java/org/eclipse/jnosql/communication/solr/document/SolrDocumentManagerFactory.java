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

import jakarta.nosql.document.DocumentManagerFactory;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.Objects;

/**
 * The solr implementation to {@link DocumentManagerFactory}
 */
public class SolrDocumentManagerFactory implements DocumentManagerFactory {

    private final HttpSolrClient solrClient;

    SolrDocumentManagerFactory(HttpSolrClient solrClient) {
        this.solrClient = solrClient;
    }

    @Override
    public SolrDocumentManager apply(String database) {
        Objects.requireNonNull(database, "database is required");
        final String baseURL = solrClient.getBaseURL() + '/' + database;
        return new DefaultSolrDocumentManager(new HttpSolrClient.Builder(baseURL).build(), database);
    }


    @Override
    public void close() {
    }
}
