/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
