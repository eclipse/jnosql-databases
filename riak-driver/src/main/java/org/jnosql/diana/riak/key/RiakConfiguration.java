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
package org.jnosql.diana.riak.key;


import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * The riak implementation to {@link KeyValueConfiguration} that returns {@link RiakKeyValueEntityManagerFactory}.
 * It tries to read diana-riak.properties file.
 * <p>riak-server-host-: The prefix to host. eg: riak-server-host-1= host1</p>
 */
public class RiakConfiguration implements KeyValueConfiguration<RiakKeyValueEntityManagerFactory> {

    private static final String SERVER_PREFIX = "riak-server-host-";
    private static final Logger LOGGER = Logger.getLogger(RiakConfiguration.class.getName());

    private static final String FILE_CONFIGURATION = "diana-riak.properties";

    private static final RiakNode DEFAULT_NODE = new RiakNode.Builder()
            .withRemoteAddress("127.0.0.1").build();

    private final List<RiakNode> nodes = new ArrayList<>();

    private RiakCluster cluster;

    public RiakConfiguration() {
        Map<String, String> properties = ConfigurationReader.from(FILE_CONFIGURATION);
        properties.keySet().stream()
                .filter(k -> k.startsWith(SERVER_PREFIX))
                .sorted().map(properties::get)
                .forEach(this::add);

    }


    /**
     * Adds new Riak node
     *
     * @param node the node
     * @throws NullPointerException when node is null
     */
    public void add(RiakNode node) throws NullPointerException {
        Objects.requireNonNull(node, "Node is required");
        this.nodes.add(node);
    }

    /**
     * Adds a new address on riak
     *
     * @param address the address to be added
     * @throws NullPointerException when address is null
     */
    public void add(String address) throws NullPointerException {
        Objects.requireNonNull(address, "Address is required");
        this.nodes.add(new RiakNode.Builder().withRemoteAddress(address).build());
    }

    @Override
    public RiakKeyValueEntityManagerFactory getManagerFactory() {

        if (nodes.isEmpty()) {
            nodes.add(DEFAULT_NODE);
        }
        cluster = new RiakCluster.Builder(nodes)
                .build();

        return new RiakKeyValueEntityManagerFactory(cluster);
    }
}
