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
import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * The MongoDB implementation to {@link UnaryDocumentConfiguration} that returns  {@link MongoDBDocumentCollectionManagerFactory}.
 * It tries to read the diana-mongodb.properties file whose has the following properties
 * <p>mongodb-server-host-: as prefix to add host client, eg: mongodb-server-host-1=host1, mongodb-server-host-2= host2</p>
 */
public class MongoDBDocumentConfiguration implements UnaryDocumentConfiguration<MongoDBDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-mongodb.properties";

    private static final int DEFAULT_PORT = 27017;

    /**
     * Creates a {@link MongoDBDocumentCollectionManagerFactory} from map configurations
     * @param configurations the configurations map
     * @return a MongoDBDocumentCollectionManagerFactory instance
     */
    public MongoDBDocumentCollectionManagerFactory get(Map<String, String> configurations) {
        List<ServerAddress> servers = configurations.keySet().stream().filter(s -> s.startsWith("mongodb-server-host-"))
                .map(configurations::get).map(HostPortConfiguration::new)
                .map(HostPortConfiguration::toServerAddress).collect(Collectors.toList());
        if (servers.isEmpty()) {
            return new MongoDBDocumentCollectionManagerFactory(new MongoClient(), MongoClients.create());
        }

        return new MongoDBDocumentCollectionManagerFactory(new MongoClient(servers), getAsyncMongoClient(servers));
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
    public MongoDBDocumentCollectionManagerFactory getAsync() {
        return get();
    }

    private class HostPortConfiguration {


        private final String host;

        private final int port;

        HostPortConfiguration(String value) {
            String[] values = value.split(":");
            if (values.length == 2) {
                host = values[0];
                port = Integer.valueOf(values[1]);
            } else {
                host = values[0];
                port = DEFAULT_PORT;
            }
        }

        public ServerAddress toServerAddress() {
            return new ServerAddress(host, port);
        }
    }
}
