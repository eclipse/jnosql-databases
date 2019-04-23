/*
 *  Copyright (c) 2019 Otávio Santana and others
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

package org.jnosql.diana.mongodb.document;


import org.jnosql.diana.api.Settings;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.HashMap;
import java.util.Map;


public enum ManagerFactorySupplier  {

    INSTANCE;

    private final GenericContainer mongodb =
            new GenericContainer("mongo:latest")
                    .withExposedPorts(27017)
                    .waitingFor(Wait.defaultWaitStrategy());

    {
        mongodb.start();
    }

    public MongoDBDocumentCollectionManager get(String database) {
        Settings settings = getSettings();
        MongoDBDocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        MongoDBDocumentCollectionManagerFactory factory = configuration.get(settings);
        return factory.get(database);
    }

    public MongoDBDocumentCollectionManagerAsync getAsync(String database) {
        Settings settings = getSettings();
        MongoDBDocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        MongoDBDocumentCollectionManagerAsyncFactory factory = configuration.getAsync(settings);
        return factory.getAsync(database);
    }

    private Settings getSettings() {
        Map<String,Object> settings = new HashMap<>();
        String host = mongodb.getContainerIpAddress() + ":" + mongodb.getFirstMappedPort();
        settings.put("mongodb.host.1", host);
        return Settings.of(settings);
    }

}
