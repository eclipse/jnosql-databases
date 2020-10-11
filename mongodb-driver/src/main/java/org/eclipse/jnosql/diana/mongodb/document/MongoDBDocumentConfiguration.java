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
package org.eclipse.jnosql.diana.mongodb.document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.document.DocumentConfiguration;
import org.eclipse.jnosql.diana.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.eclipse.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.URL;


/**
 * The MongoDB implementation to {@link DocumentConfiguration}
 * that returns  {@link MongoDBDocumentCollectionManagerFactory}
 * It tries to read the diana-mongodb.properties file whose has the following properties
 * <p>mongodb.server.host.: as prefix to add host client, eg: mongodb.server.host.1=host1, mongodb.server.host.2= host2</p>
 */
public class MongoDBDocumentConfiguration implements DocumentConfiguration {

    static final String FILE_CONFIGURATION = "diana-mongodb.properties";

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
            Optional<ConnectionString> connectionString = settings
                    .get(URL.get(), String.class)
                    .map(ConnectionString::new);

            return connectionString.map(c -> MongoClientSettings.builder()
                    .applyConnectionString(c)
                    .retryWrites(true)
                    .build())
                    .map(MongoClients::create)
                    .map(MongoDBDocumentCollectionManagerFactory::new)
                    .orElseGet(() -> new MongoDBDocumentCollectionManagerFactory(MongoClients.create()));
        }

        Optional<MongoCredential> credential = MongoAuthentication.of(settings);


        final MongoClientSettings mongoClientSettings = credential.map(c -> MongoClientSettings.builder().credential(c)
                .applyToClusterSettings(builder -> builder.hosts(servers))).orElseGet(() ->
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder -> builder.hosts(servers))).build();

        return new MongoDBDocumentCollectionManagerFactory(MongoClients.create(mongoClientSettings));
    }

    public MongoDBDocumentCollectionManagerFactory get(String pathFileConfig) throws NullPointerException {
        requireNonNull(pathFileConfig, "settings is required");

        Map<String, String> configuration = ConfigurationReader.from(pathFileConfig);
        return get(configuration);
    }


}
