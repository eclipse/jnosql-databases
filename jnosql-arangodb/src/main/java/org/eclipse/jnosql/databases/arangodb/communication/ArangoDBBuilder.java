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

import com.arangodb.ArangoDB;
import com.arangodb.ContentType;
import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;
import com.arangodb.serde.jackson.JacksonSerde;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This interface defines methods for building configurations for ArangoDB connections.
 */
public final class ArangoDBBuilder {

    private final ArangoDB.Builder arangoDB;

    private final List<EntrySerializer<?>> serializers = new ArrayList<>();

    private final List<EntryDeserializer<?>> deserializers = new ArrayList<>();

    ArangoDBBuilder(ArangoDB.Builder arangoDB) {
        this.arangoDB = arangoDB;
    }

    /**
     * Sets the host and port for the ArangoDB connection.
     *
     * @param host The host name or IP address.
     * @param port The port number.
     */
    public void host(String host, int port){
        arangoDB.host(host, port);
    }

    /**
     * Sets the timeout for ArangoDB operations.
     *
     * @param timeout The timeout value in milliseconds.
     */
   public void timeout(int timeout){
        arangoDB.timeout(timeout);
    }

    /**
     * Sets the username for authentication.
     *
     * @param user The username.
     */
    public void user(String user){
        arangoDB.user(user);
    }


    /**
     * Sets the password for authentication.
     *
     * @param password The password.
     */
    public void password(String password) {
        arangoDB.password(password);
    }

    /**
     * Specifies whether to use SSL for the ArangoDB connection.
     *
     * @param useSsl true to use SSL, false otherwise.
     */
    public void useSsl(boolean useSsl){
        arangoDB.useSsl(useSsl);
    }

    /**
     * Sets the chunk size for data transfers.
     *
     * @param chunkSize The chunk size in bytes.
     */
    public void chunkSize(int chunkSize){
        arangoDB.chunkSize(chunkSize);
    }

    /**
     * Sets the maximum number of connections allowed.
     *
     * @param maxConnections The maximum number of connections.
     */
    public void maxConnections(int maxConnections) {
        arangoDB.maxConnections(maxConnections);
    }

    /**
     * Sets the protocol to be used for the ArangoDB connection.
     *
     * @param protocol The protocol.
     */
    public void protocol(Protocol protocol){
        arangoDB.protocol(protocol);
    }

    /**
     * Specifies whether to acquire the list of available hosts.
     *
     * @param acquireHostList true to acquire host list, false otherwise.
     */
    public void acquireHostList(boolean acquireHostList){
        arangoDB.acquireHostList(acquireHostList);
    }

    /**
     * Sets the load balancing strategy for the connection.
     *
     * @param loadBalancingStrategy The load balancing strategy.
     */
    public void loadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy) {
        arangoDB.loadBalancingStrategy(loadBalancingStrategy);
    }

    /**
     * Adds an entry deserializer to the builder.
     * @param <T> The type of the entry deserializer.
     * @param serializer The entry deserializer to add.
     */
    public <T> void add(EntrySerializer<T> serializer){
        Objects.requireNonNull(serializer, "deserializer is required");
        this.serializers.add(serializer);
    }

    /**
     * Adds an entry deserializer to the builder.
     * @param <T> The type of the entry deserializer.
     * @param deserializer The entry deserializer to add.
     */
    public <T> void add(EntryDeserializer<T> deserializer){
        Objects.requireNonNull(deserializer, "deserializer is required");
        this.deserializers.add(deserializer);
    }

    /**
     * Checks if the builder has a deserializer.
     *
     * @return true if a deserializer has been added, false otherwise.
     */
    public boolean hasSerializer(){
        return !serializers.isEmpty() || !deserializers.isEmpty();
    }

    /**
     * Builds the ArangoDB connection.
     *
     * @return The ArangoDB connection.
     */
    public ArangoDB build() {
        if (hasSerializer()) {
            JacksonSerde serde = JacksonSerde.of(ContentType.JSON);
            serde.configure(mapper -> {
                SimpleModule module = new SimpleModule("JNoSQLModule");

                serializers.forEach(s -> serializer(module, s));
                deserializers.forEach(d -> deserializer(module, d));
                mapper.registerModule(module);
            });
        }
        return arangoDB.build();
    }

    private <T> void serializer(SimpleModule module, EntrySerializer<T> serializer) {
        module.addSerializer(serializer.type(), serializer.serializer());
    }

    private <T> void deserializer(SimpleModule module, EntryDeserializer<T> serializer) {
        module.addDeserializer(serializer.type(), serializer.deserializer());
    }

}
