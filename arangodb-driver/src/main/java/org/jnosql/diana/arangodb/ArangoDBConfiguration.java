/*
 * Copyright 2017 Otavio Santana and others
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
package org.jnosql.diana.arangodb;


import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;

import java.util.Objects;

/**
 * The base to configuration both key-value and document on mongoDB.
 * To each configuration setted, it will change both builder
 * {@link ArangoDB.Builder} and {@link ArangoDBAsync.Builder}
 */
public abstract class ArangoDBConfiguration {


    protected ArangoDB.Builder builder = new ArangoDB.Builder();

    protected ArangoDBAsync.Builder builderAsync = new ArangoDBAsync.Builder();


    /**
     * set the setHost
     *
     * @param host the setHost
     */
    public void setHost(String host) {
        builder.host(host);
        builderAsync.host(host);
    }

    /**
     * set the setPort
     *
     * @param port the setPort
     */
    public void setPort(int port) {
        builder.port(port);
        builderAsync.port(port);
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
        Objects.requireNonNull(builder, "builder is required");
        this.builder = builder;
    }

    /**
     * Defines a new asyncBuilder to ArangoDB
     *
     * @param builderAsync the new builderAsync
     * @throws NullPointerException when builderAsync is null
     */
    public void asyncBuilder(ArangoDBAsync.Builder builderAsync) throws NullPointerException {
        Objects.requireNonNull(builderAsync, "asyncBuilder is required");
        this.builderAsync = builderAsync;
    }

}
