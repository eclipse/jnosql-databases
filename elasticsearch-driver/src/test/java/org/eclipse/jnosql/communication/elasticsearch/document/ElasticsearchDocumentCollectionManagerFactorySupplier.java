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
package org.eclipse.jnosql.communication.elasticsearch.document;

import jakarta.nosql.Settings;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

enum ElasticsearchDocumentCollectionManagerFactorySupplier implements Supplier<ElasticsearchDocumentCollectionManagerFactory> {

    INSTANCE;

    private final GenericContainer es =
            new GenericContainer("elasticsearch:7.5.1")
                    .withExposedPorts(9200, 9300)
                    .withEnv("discovery.type", "single-node")
                    .waitingFor(Wait.forHttp("/")
                            .forPort(9200)
                            .forStatusCode(200));
    {
        es.start();
    }


    @Override
    public ElasticsearchDocumentCollectionManagerFactory get() {
        ElasticsearchDocumentConfiguration configuration = new ElasticsearchDocumentConfiguration();
        Map<String, Object> settings = new HashMap<>();
        settings.put("elasticsearch.host.1", es.getContainerIpAddress() + ':' + es.getFirstMappedPort());
        settings.put("elasticsearch.cluster.name", "elasticsearch");
        return configuration.get(Settings.of(settings));
    }

}
