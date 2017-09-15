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
package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private static final String SETTINGS_PREFIX = "elasticsearch-settings-";

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
                .map(h -> ElastissearchAdress.of(configurations.get(h), DEFAULT_PORT))
                .map(ElastissearchAdress::toTransportAddress)
                .forEach(hosts::add);

        settings.putAll(configurations
                .keySet()
                .stream()
                .filter(k -> k.startsWith(SETTINGS_PREFIX))
                .collect(toMap(k -> k.replace(SETTINGS_PREFIX, ""), k -> configurations.get(k))));
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
     * @param address the address
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
    public ElasticsearchDocumentCollectionManagerFactory get(org.jnosql.diana.api.Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");

        Map<String, String> configurations = new HashMap<>();
        settings.entrySet().forEach(e -> configurations.put(e.getKey(), e.getValue().toString()));

        List<TransportAddress> hosts = new ArrayList<>();

        String name = configurations.getOrDefault("elasticsearch-cluster-name", DEFAULT_CLUSTER_NAME);

        configurations.keySet().stream()
                .filter(k -> k.startsWith(HOST_PREFIX))
                .sorted()
                .map(h -> ElastissearchAdress.of(configurations.get(h), DEFAULT_PORT))
                .map(ElastissearchAdress::toTransportAddress)
                .forEach(hosts::add);

        Settings.Builder builder = Settings.builder();
        builder.put("cluster.name", name);

        configurations.keySet().stream().filter(k -> k.startsWith(SETTINGS_PREFIX))
                .collect(toMap(k -> k.replace(SETTINGS_PREFIX, ""), k -> configurations.get(k)))
                .forEach((k, v) -> builder.put(k, v));

        PreBuiltTransportClient transportClient = new PreBuiltTransportClient(builder.build());
        hosts.forEach(transportClient::addTransportAddress);
        return new ElasticsearchDocumentCollectionManagerFactory(transportClient);
    }


    @Override
    public ElasticsearchDocumentCollectionManagerFactory getAsync() throws UnsupportedOperationException {
        return get();
    }

    @Override
    public ElasticsearchDocumentCollectionManagerFactory getAsync(org.jnosql.diana.api.Settings settings) throws NullPointerException {
        return get(settings);
    }

    /**
     * Returns an {@link ElasticsearchDocumentCollectionManagerFactory} instance from {@link Settings}
     *
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
