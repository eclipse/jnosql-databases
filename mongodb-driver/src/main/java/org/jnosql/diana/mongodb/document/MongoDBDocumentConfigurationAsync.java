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

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.connection.ClusterSettings;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfigurationAsync;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfiguration.FILE_CONFIGURATION;

/**
 * The MongoDB implementation to {@link DocumentConfigurationAsync}
 * that returns {@link MongoDBDocumentCollectionManagerAsyncFactory}.
 * It tries to read the diana-mongodb.properties file whose has the following properties
 * <p>mongodb.server.host.: as prefix to add host client, eg: mongodb.server.host.1=host1, mongodb.server.host.2= host2</p>
 */
public class MongoDBDocumentConfigurationAsync implements DocumentConfigurationAsync {


    @Override
    public MongoDBDocumentCollectionManagerAsyncFactory get() {
        Map<String, String> configurations = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(configurations);
    }

    @Override
    public MongoDBDocumentCollectionManagerAsyncFactory get(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");
        List<ServerAddress> servers = settings
                .prefix(Arrays.asList(OldMongoDBDocumentConfigurations.HOST.get(), MongoDBDocumentConfigurations.HOST.get(),
                        Configurations.HOST.get()))
                .stream()
                .map(Object::toString)
                .map(HostPortConfiguration::new)
                .map(HostPortConfiguration::toServerAddress)
                .collect(Collectors.toList());

        if (servers.isEmpty()) {
            return new MongoDBDocumentCollectionManagerAsyncFactory(MongoClients.create());
        }
        return new MongoDBDocumentCollectionManagerAsyncFactory(getAsyncMongoClient(servers));
    }

    /**
     * Creates a {@link MongoDBDocumentCollectionManagerAsyncFactory} from mongoClient
     *
     * @param mongoClient the mongo client {@link MongoClient}
     * @return a MongoDBDocumentCollectionManagerAsyncFactory instance
     * @throws NullPointerException when the mongoClient is null
     */
    public MongoDBDocumentCollectionManagerAsyncFactory get(com.mongodb.async.client.MongoClient mongoClient) throws NullPointerException {
        requireNonNull(mongoClient, "mongo client is required");
        return new MongoDBDocumentCollectionManagerAsyncFactory(mongoClient);
    }

    private com.mongodb.async.client.MongoClient getAsyncMongoClient(List<ServerAddress> servers) {
        ClusterSettings clusterSettings = ClusterSettings.builder().hosts(servers).build();
        MongoClientSettings settings = MongoClientSettings.builder().clusterSettings(clusterSettings).build();
        return MongoClients.create(settings);
    }

    private MongoDBDocumentCollectionManagerAsyncFactory get(Map<String, String> configurations) {
        Objects.requireNonNull(configurations, "configurations is required");
        Settings.SettingsBuilder builder = Settings.builder();
        configurations.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }
}
