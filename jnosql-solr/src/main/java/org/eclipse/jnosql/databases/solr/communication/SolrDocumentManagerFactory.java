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

package org.eclipse.jnosql.databases.solr.communication;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.eclipse.jnosql.communication.semistructured.DatabaseManagerFactory;

import java.util.Objects;

/**
 * The solr implementation to {@link DatabaseManagerFactory}
 */
public class SolrDocumentManagerFactory implements DatabaseManagerFactory {

    private final Http2SolrClient solrClient;

    private final boolean automaticCommit;

    SolrDocumentManagerFactory(Http2SolrClient solrClient, boolean automaticCommit) {
        this.solrClient = solrClient;
        this.automaticCommit = automaticCommit;
    }

    @Override
    public SolrDocumentManager apply(String database) {
        Objects.requireNonNull(database, "database is required");
        final String baseURL = solrClient.getBaseURL() + '/' + database;
        return new DefaultSolrDocumentManager(new Http2SolrClient.Builder(baseURL).build(), database, automaticCommit);
    }


    @Override
    public void close() {
    }
}
