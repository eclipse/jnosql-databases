/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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

import org.eclipse.jnosql.communication.Settings;
import org.testcontainers.containers.SolrContainer;

import java.util.function.Supplier;

enum DocumentDatabase implements Supplier<SolrDocumentManager> {
    INSTANCE;

    private static final String SOLR_IMAGE = "solr:9.1.1";
    private static final String COLLECTION = "database";
    private final SolrContainer container;

    {
        container = new SolrContainer(SOLR_IMAGE)
                .withCollection(COLLECTION);
        container.start();
    }

    @Override
    public SolrDocumentManager get() {
        SolrDocumentConfiguration configuration = new SolrDocumentConfiguration();
        final SolrDocumentManagerFactory managerFactory = configuration.apply(getSettings());
        return managerFactory.apply(COLLECTION);
    }

    private Settings getSettings() {
        return Settings.builder()
                .put(SolrDocumentConfigurations.HOST.get(), DocumentDatabase.INSTANCE.getHost())
                .build();
    }

    private String getHost() {
        return "http://" + container.getHost() + ":" + container.getSolrPort()+  "/solr";
    }


}
