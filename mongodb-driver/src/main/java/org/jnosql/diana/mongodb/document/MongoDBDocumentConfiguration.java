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

package org.jnosql.diana.mongodb.document;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.connection.ClusterSettings;
import org.jnosql.diana.api.Configurations;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.SettingsBuilder;
import org.jnosql.diana.api.document.DocumentConfiguration;
import org.jnosql.diana.api.document.DocumentConfigurationAsync;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


/**
 * The MongoDB implementation to both {@link DocumentConfiguration} and {@link DocumentConfigurationAsync}
 * that returns  {@link MongoDBDocumentCollectionManagerFactory} {@link MongoDBDocumentCollectionManagerAsyncFactory}.
 * It tries to read the diana-mongodb.properties file whose has the following properties
 * <p>mongodb.server.host.: as prefix to add host client, eg: mongodb.server.host.1=host1, mongodb.server.host.2= host2</p>
 */
public class MongoDBDocumentConfiguration implements DocumentConfiguration<MongoDBDocumentCollectionManagerFactory>,
        DocumentConfigurationAsync<MongoDBDocumentCollectionManagerAsyncFactory> {

    private static final String FILE_CONFIGURATION = "diana-mongodb.properties";

    static final int DEFAULT_PORT = 27017;


    /**
     * Creates a {@link MongoDBDocumentCollectionManagerFactory} from map configurations
     *
     * @param configurations the configurations map
     * @return a MongoDBDocumentCollectionManagerFactory instance
     * @throws NullPointerException when the configurations is null
     */
    public MongoDBDocumentCollectionManagerFactory get(Map<String, String> configurations) throws NullPointerException {
        requireNonNull(configurations, "configurations is required");
        SettingsBuilder builder = Settings.builder();
        configurations.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }

    /**
     * Creates a {@link MongoDBDocumentCollectionManagerFactory} from mongoClient
     *
     * @param mongoClient the mongo client {@link MongoClient}
     * @return a MongoDBDocumentCollectionManagerFactory instance
     * @throws NullPointerException when the mongoClient is null
     */
    public MongoDBDocumentCollectionManagerFactory get(MongoClient mongoClient) throws NullPointerException {
        requireNonNull(mongoClient, "mongo client is required");
        return new MongoDBDocumentCollectionManagerFactory(mongoClient);
    }

    private com.mongodb.async.client.MongoClient getAsyncMongoClient(List<ServerAddress> servers) {
        ClusterSettings clusterSettings = ClusterSettings.builder().hosts(servers).build();
        MongoClientSettings settings = MongoClientSettings.builder().clusterSettings(clusterSettings).build();
        return MongoClients.create(settings);
    }


    @Override
    public MongoDBDocumentCollectionManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(configuration);

    }

    @Override
    public MongoDBDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
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
            return new MongoDBDocumentCollectionManagerFactory(new MongoClient());
        }

        return new MongoDBDocumentCollectionManagerFactory(new MongoClient(servers));
    }

    public MongoDBDocumentCollectionManagerFactory get(String pathFileConfig) throws NullPointerException {
        requireNonNull(pathFileConfig, "settings is required");

        Map<String, String> configuration = ConfigurationReader.from(pathFileConfig);
        return get(configuration);
    }


    @Override
    public MongoDBDocumentCollectionManagerAsyncFactory getAsync() {
        Map<String, String> configurations = ConfigurationReader.from(FILE_CONFIGURATION);
        return getAsync(configurations);
    }


    @Override
    public MongoDBDocumentCollectionManagerAsyncFactory getAsync(Settings settings) throws NullPointerException {
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
    public MongoDBDocumentCollectionManagerAsyncFactory getAsync(com.mongodb.async.client.MongoClient mongoClient) throws NullPointerException {
        requireNonNull(mongoClient, "mongo client is required");
        return new MongoDBDocumentCollectionManagerAsyncFactory(mongoClient);
    }


    private MongoDBDocumentCollectionManagerAsyncFactory getAsync(Map<String, String> configurations) {
        Objects.requireNonNull(configurations, "configurations is required");
        SettingsBuilder builder = Settings.builder();
        configurations.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return getAsync(builder.build());
    }

}
