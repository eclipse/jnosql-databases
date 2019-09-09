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
package org.eclipse.jnosql.diana.arangodb;


import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import com.arangodb.entity.LoadBalancingStrategy;
import jakarta.nosql.Settings;

import static java.util.Objects.requireNonNull;

/**
 * The base to configuration both key-value and document on mongoDB.
 * To each configuration setted, it will change both builder
 * {@link ArangoDB.Builder} and {@link ArangoDBAsync.Builder}
 */
public abstract class ArangoDBConfiguration {


    protected ArangoDB.Builder builder = new ArangoDB.Builder();

    protected ArangoDBAsync.Builder builderAsync = new ArangoDBAsync.Builder();

    /**
     * Adds a host in the arangodb builder
     *
     * @param host the host
     * @param port the port
     * @throws NullPointerException when host is null
     */
    public void addHost(String host, int port) throws NullPointerException {
        requireNonNull(host, "host is required");
        builder.host(host, port);
        builderAsync.host(host, port);
    }

    /**
     * Set the {@link LoadBalancingStrategy}
     *
     * @param loadBalancingStrategy the LoadBalancingStrategy
     * @throws NullPointerException when the loadBalancingStrategy is null
     */
    public void setLoadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy) throws NullPointerException {
        requireNonNull(loadBalancingStrategy, "loadBalancingStrategy is required");
        builder.loadBalancingStrategy(loadBalancingStrategy);
        builderAsync.loadBalancingStrategy(loadBalancingStrategy);
    }


    /**
     * set the setTimeout
     *
     * @param timeout the setTimeout
     */
    public void setTimeout(int timeout) {
        builder.timeout(timeout);
        builderAsync.timeout(timeout);
    }

    /**
     * set the setUser
     *
     * @param user the setUser
     */
    public void setUser(String user) {
        builder.user(user);
        builderAsync.user(user);
    }

    /**
     * set the setPassword
     *
     * @param password the setPassword
     */
    public void setPassword(String password) {
        builder.password(password);
        builderAsync.password(password);
    }

    /**
     * set if gonna use ssl
     *
     * @param value the ssl
     */
    public void setUseSSL(boolean value) {
        builder.useSsl(value);
        builderAsync.useSsl(value);
    }

    /**
     * set the chucksize
     *
     * @param chuckSize the cucksize
     */
    public void setChuckSize(int chuckSize) {
        builder.chunksize(chuckSize);
        builderAsync.chunksize(chuckSize);
    }

    /**
     * Defines a new builder to sync ArangoDB
     *
     * @param builder the new builder
     * @throws NullPointerException when builder is null
     */
    public void syncBuilder(ArangoDB.Builder builder) throws NullPointerException {
        requireNonNull(builder, "builder is required");
        this.builder = builder;
    }

    /**
     * Defines a new asyncBuilder to ArangoDB
     *
     * @param builderAsync the new builderAsync
     * @throws NullPointerException when builderAsync is null
     */
    public void asyncBuilder(ArangoDBAsync.Builder builderAsync) throws NullPointerException {
        requireNonNull(builderAsync, "asyncBuilder is required");
        this.builderAsync = builderAsync;
    }

    protected ArangoDB getArangoDB(Settings settings) {
        ArangoDBBuilderSync aragonDB = new ArangoDBBuilderSync(builder);
        ArangoDBBuilders.load(settings, aragonDB);
        return aragonDB.build();
    }

    protected ArangoDBAsync getArangoDBAsync(Settings settings) {
        ArangoDBBuilderAsync aragonDB = new ArangoDBBuilderAsync(builderAsync);
        ArangoDBBuilders.load(settings, aragonDB);
        return aragonDB.build();
    }

}
