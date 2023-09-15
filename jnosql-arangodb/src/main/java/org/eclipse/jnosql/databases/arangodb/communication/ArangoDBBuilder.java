/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.arangodb.communication;

import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;

/**
 * This interface defines methods for building configurations for ArangoDB connections.
 */
public interface ArangoDBBuilder {

    /**
     * Sets the host and port for the ArangoDB connection.
     *
     * @param host The host name or IP address.
     * @param port The port number.
     */
    void host(String host, int port);

    /**
     * Sets the timeout for ArangoDB operations.
     *
     * @param timeout The timeout value in milliseconds.
     */
    void timeout(int timeout);

    /**
     * Sets the username for authentication.
     *
     * @param user The username.
     */
    void user(String user);

    /**
     * Sets the password for authentication.
     *
     * @param password The password.
     */
    void password(String password);

    /**
     * Specifies whether to use SSL for the ArangoDB connection.
     *
     * @param useSsl true to use SSL, false otherwise.
     */
    void useSsl(boolean useSsl);

    /**
     * Sets the chunk size for data transfers.
     *
     * @param chunkSize The chunk size in bytes.
     */
    void chunkSize(int chunkSize);

    /**
     * Sets the maximum number of connections allowed.
     *
     * @param maxConnections The maximum number of connections.
     */
    void maxConnections(int maxConnections);

    /**
     * Sets the protocol to be used for the ArangoDB connection.
     *
     * @param protocol The protocol.
     */
    void protocol(Protocol protocol);

    /**
     * Specifies whether to acquire the list of available hosts.
     *
     * @param acquireHostList true to acquire host list, false otherwise.
     */
    void acquireHostList(boolean acquireHostList);

    /**
     * Sets the load balancing strategy for the connection.
     *
     * @param loadBalancingStrategy The load balancing strategy.
     */
    void loadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy);

    /**
     * Adds an entry serializer to the builder.
     *
     * @param serializer The entry serializer to add.
     */
    void add(EntrySerializer<?> serializer);

    /**
     * Adds an entry deserializer to the builder.
     *
     * @param deserializer The entry deserializer to add.
     */
    void add(EntryDeserializer<?> deserializer);

    /**
     * Checks if the builder has a serializer.
     *
     * @return true if a serializer has been added, false otherwise.
     */
    boolean hasSerializer();
}
