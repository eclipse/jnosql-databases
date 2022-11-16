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

import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfiguration;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;


/**
 * The Apache Solr implementation to {@link DocumentConfiguration}
 * that returns  {@link SolrDocumentManagerFactory}
 * @see SolrDocumentConfigurations
 */
public class SolrDocumentConfiguration implements DocumentConfiguration {


    private static final String DEFAULT_HOST = "http://localhost:8983/solr/";

    /**
     * Creates a {@link SolrDocumentManagerFactory} from mongoClient
     *
     * @param solrClient the mongo client {@link HttpSolrClient}
     * @return a SolrDocumentCollectionManagerFactory instance
     * @throws NullPointerException when the mongoClient is null
     */
    public SolrDocumentManagerFactory get(HttpSolrClient solrClient) throws NullPointerException {
        requireNonNull(solrClient, "solrClient is required");
        return new SolrDocumentManagerFactory(solrClient, true);
    }


    @Override
    public SolrDocumentManagerFactory apply(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");


        String host = settings.getSupplier(Arrays.asList(SolrDocumentConfigurations.HOST,
                        Configurations.HOST)).map(Object::toString).orElse(DEFAULT_HOST);

        boolean automaticCommit = settings.getOrDefault(SolrDocumentConfigurations.AUTOMATIC_COMMIT, true);

        final HttpSolrClient solrClient = new HttpSolrClient.Builder(host).build();
        solrClient.setParser(new XMLResponseParser());
        return new SolrDocumentManagerFactory(solrClient, automaticCommit);

    }


}
