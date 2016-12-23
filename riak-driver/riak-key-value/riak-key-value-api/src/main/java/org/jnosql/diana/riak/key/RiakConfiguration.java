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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RiakConfiguration implements KeyValueConfiguration<RiakKeyValueEntityManagerFactory> {

    private final List<RiakNode> nodes = new ArrayList<>();

    private RiakCluster cluster;

    private static final RiakNode DEFAULT_NODE = new RiakNode.Builder()
            .withRemoteAddress("127.0.0.1")
            .withRemotePort(10017).build();


    public void add(RiakNode node) {
        Objects.requireNonNull(node, "Node is required");
        this.nodes.add(node);
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
