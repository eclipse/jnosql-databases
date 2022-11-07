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
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;


/**
 * The Apache Solr implementation to {@link DocumentConfiguration}
 * that returns  {@link SolrDocumentCollectionManagerFactory}
 * @see SolrDocumentConfigurations
 */
public class SolrDocumentConfiguration implements DocumentConfiguration {

    static final String FILE_CONFIGURATION = "diana-solr.properties";


    private static final String DEFAULT_HOST = "http://localhost:8983/solr/";

    /**
     * Creates a {@link SolrDocumentCollectionManagerFactory} from mongoClient
     *
     * @param solrClient the mongo client {@link HttpSolrClient}
     * @return a SolrDocumentCollectionManagerFactory instance
     * @throws NullPointerException when the mongoClient is null
     */
    public SolrDocumentCollectionManagerFactory get(HttpSolrClient solrClient) throws NullPointerException {
        requireNonNull(solrClient, "solrClient is required");
        return new SolrDocumentCollectionManagerFactory(solrClient);
    }

    @Override
    public SolrDocumentCollectionManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(Settings.of(new HashMap<>(configuration)));

    }

    @Override
    public SolrDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");


        String host = settings.get(Arrays.asList(SolrDocumentConfigurations.HOST.get(),
                        Configurations.HOST.get())).map(Object::toString).orElse(DEFAULT_HOST);

        final HttpSolrClient solrClient = new HttpSolrClient.Builder(host).build();
        solrClient.setParser(new XMLResponseParser());
        return new SolrDocumentCollectionManagerFactory(solrClient);

    }


}
