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
package org.jnosql.diana.solr.document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.document.DocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


/**
 * The MongoDB implementation to {@link DocumentConfiguration}
 * that returns  {@link MongoDBDocumentCollectionManagerFactory}
 * It tries to read the diana-solr.properties file whose has the following properties
 * <p>solr.server.host.: as prefix to add host client, eg: solr.server.host.1=host1, solr.server.host.2= host2</p>
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
            return new MongoDBDocumentCollectionManagerFactory(new MongoClient());
        }

        Optional<MongoCredential> credential = MongoAuthentication.of(settings);
        MongoClient mongoClient = credential.map(c -> new MongoClient(servers, c, MongoClientOptions.builder().build()))
                .orElseGet(() -> new MongoClient(servers));

        return new MongoDBDocumentCollectionManagerFactory(mongoClient);
    }

    public MongoDBDocumentCollectionManagerFactory get(String pathFileConfig) throws NullPointerException {
        requireNonNull(pathFileConfig, "settings is required");

        Map<String, String> configuration = ConfigurationReader.from(pathFileConfig);
        return get(configuration);
    }


}
