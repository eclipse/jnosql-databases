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
package org.jnosql.diana.arangodb;


import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import org.jnosql.diana.api.Settings;

import java.util.Objects;

import static java.util.Optional.ofNullable;

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

    protected ArangoDB getArangoDB(Settings settings) {
        ArangoDB.Builder arangoDB = new ArangoDB.Builder();
        ofNullable(settings.get("arangodb-host")).map(Object::toString).ifPresent(arangoDB::host);
        ofNullable(settings.get("arangodb-user")).map(Object::toString).ifPresent(arangoDB::user);
        ofNullable(settings.get("arangodb-password")).map(Object::toString).ifPresent(arangoDB::password);

        ofNullable(settings.get("arangodb-port")).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get("arangodb-timeout")).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::timeout);
        ofNullable(settings.get("arangodb-chuckSize")).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get("arangodb-userSsl")).map(Object::toString).map(Boolean::valueOf).ifPresent(arangoDB::useSsl);
        return arangoDB.build();
    }

    protected ArangoDBAsync getArangoDBAsync(Settings settings) {
        ArangoDBAsync.Builder arangoDB = new ArangoDBAsync.Builder();

        ofNullable(settings.get("arangodb-host")).map(Object::toString).ifPresent(arangoDB::host);
        ofNullable(settings.get("arangodb-user")).map(Object::toString).ifPresent(arangoDB::user);
        ofNullable(settings.get("arangodb-password")).map(Object::toString).ifPresent(arangoDB::password);

        ofNullable(settings.get("arangodb-port")).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get("arangodb-timeout")).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::timeout);
        ofNullable(settings.get("arangodb-chuckSize")).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get("arangodb-userSsl")).map(Object::toString).map(Boolean::valueOf).ifPresent(arangoDB::useSsl);
        return arangoDB.build();
    }

}
