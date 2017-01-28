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
package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * The implementation of {@link UnaryDocumentConfiguration} that returns {@link ElasticsearchDocumentCollectionManagerFactory}.
 * It tries to read the configuration properties from diana-elasticsearch.properties file. To get some information:
 * <p>elasticsearch-host-n: the host to client connection, if necessary to define a different port than default just
 * use colon, ':' eg: elasticsearch-host-1=172.17.0.2:1234</p>
 * <p>elasticsearch-settings-n: the is a prefix to put some configuration on elastisearch to be use {@link Settings.Builder#put(String, String)}</p>
 */
public class ElasticsearchDocumentConfiguration implements UnaryDocumentConfiguration<ElasticsearchDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-elasticsearch.properties";
    private static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
    private static final String HOST_PREFIX = "elasticsearch-host-";
    private static final int DEFAULT_PORT = 9300;
    public static final String SETTINGS_PREFIX = "elasticsearch-settings-";

    private String clusterName;

    private List<TransportAddress> hosts = new ArrayList<>();

    private Map<String, String> settings = new HashMap<>();

    public ElasticsearchDocumentConfiguration() {
        Map<String, String> configurations = ConfigurationReader.from(FILE_CONFIGURATION);
        if (configurations.isEmpty()) {
            return;
        }
        this.clusterName = configurations.getOrDefault("elasticsearch-cluster-name", DEFAULT_CLUSTER_NAME);
        configurations.keySet().stream()
                .filter(k -> k.startsWith(HOST_PREFIX))
                .sorted()
                .map(h -> ElastissearchAdress.of(h, DEFAULT_PORT))
                .map(ElastissearchAdress::toTransportAddress)
                .forEach(hosts::add);

        settings.putAll(configurations
                .keySet()
                .stream()
                .filter(k -> k.startsWith(SETTINGS_PREFIX))
                .collect(toMap(k -> k.replace(SETTINGS_PREFIX, ""),
                        k -> configurations.get(k))));
    }


    /**
     * Sets the cluster name
     *
     * @param clusterName the cluster name to client
     * @throws NullPointerException when the clustername is null
     */
    public void setClusterName(String clusterName) throws NullPointerException {
        this.clusterName = requireNonNull(clusterName, "clusterName is required");
    }

    /**
     * Adds a new address to elasticsearch client
     *
     * @param address the adress
     * @throws NullPointerException when address is null
     */
    public void add(TransportAddress address) throws NullPointerException {
        hosts.add(requireNonNull(address, "address is required"));
    }

    /**
     * Adds settings to be used on Elasticsearch
     *
     * @param key   the key
     * @param value the value
     * @throws NullPointerException when either key or valeu are null
     * @see Settings.Builder#put(String, String)
     */
    public void addSettings(String key, String value) throws NullPointerException {
        this.settings.put(requireNonNull(key, "key is required"), requireNonNull(value, "value is required"));
    }

    @Override
    public ElasticsearchDocumentCollectionManagerFactory get() throws UnsupportedOperationException {
        Settings settings = getSettings();
        return getFactory(settings);
    }


    @Override
    public ElasticsearchDocumentCollectionManagerFactory getAsync() throws UnsupportedOperationException {
        return get();
    }

    /**
     * Returns an {@link ElasticsearchDocumentCollectionManagerFactory} instance from {@link Settings}
     * @param settings the settins
     * @return the ElasticsearchDocumentCollectionManagerFactory instance
     * @throws NullPointerException when settins is null
     */
    public ElasticsearchDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
        return getFactory(requireNonNull(settings, "settings is required"));
    }



    private ElasticsearchDocumentCollectionManagerFactory getFactory(Settings settings) {
        PreBuiltTransportClient transportClient = new PreBuiltTransportClient(settings);
        hosts.forEach(transportClient::addTransportAddress);
        return new ElasticsearchDocumentCollectionManagerFactory(transportClient);
    }

    private Settings getSettings() {
        Settings.Builder builder = Settings.builder();

        if (Objects.nonNull(clusterName)) {
            builder.put("cluster.name", this.clusterName);
        }
        return builder.build();
    }


}
