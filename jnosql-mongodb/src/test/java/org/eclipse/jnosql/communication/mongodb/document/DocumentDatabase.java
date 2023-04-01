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

package org.eclipse.jnosql.communication.mongodb.document;


import org.eclipse.jnosql.communication.Settings;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.HashMap;
import java.util.Map;


public enum DocumentDatabase {

    INSTANCE;

    private final GenericContainer<?> mongodb =
            new GenericContainer<>("mongo:latest")
                    .withExposedPorts(27017)
                    .waitingFor(Wait.defaultWaitStrategy());

    {
        mongodb.start();
    }

    public MongoDBDocumentManager get(String database) {
        Settings settings = getSettings();
        MongoDBDocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        MongoDBDocumentManagerFactory factory = configuration.apply(settings);
        return factory.apply(database);
    }


    private Settings getSettings() {
        Map<String,Object> settings = new HashMap<>();
        String host = mongodb.getHost() + ":" + mongodb.getFirstMappedPort();
        settings.put(MongoDBDocumentConfigurations.HOST.get()+".1", host);
        return Settings.of(settings);
    }

}
