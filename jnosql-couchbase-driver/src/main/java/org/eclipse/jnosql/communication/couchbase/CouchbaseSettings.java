/*
 *  Copyright (c) 2022 Otávio Santana and others
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
package org.eclipse.jnosql.communication.couchbase;

import com.couchbase.client.java.Cluster;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable structure that has the Couchbase settings.
 */
public final class CouchbaseSettings {

    private final String host;

    private final String user;

    private final String password;

    private final String scope;

    private final String index;

    private final String collection;
    private final List<String> collections;

    CouchbaseSettings(String host, String user, String password,
                      String scope, String index, String collection,
                      List<String> collections) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.scope = scope;
        this.index = index;
        this.collection = collection;
        this.collections = collections;
    }


    /**
     * Returns the host {@link jakarta.nosql.Configurations#HOST} or {@link CouchbaseConfigurations#HOST}
     *
     * @return the host {@link jakarta.nosql.Configurations#HOST} or {@link CouchbaseConfigurations#HOST
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the user {@link jakarta.nosql.Configurations#USER} or {@link CouchbaseConfigurations#USER}
     *
     * @return the user {@link jakarta.nosql.Configurations#USER} or {@link CouchbaseConfigurations#USER}
     */
    public String getUser() {
        return user;
    }

    /**
     * Returns the password {@link jakarta.nosql.Configurations#PASSWORD} or {@link CouchbaseConfigurations#PASSWORD}
     *
     * @return the password {@link jakarta.nosql.Configurations#PASSWORD} or {@link CouchbaseConfigurations#PASSWORD}
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns the scope {@link CouchbaseConfigurations#SCOPE}
     *
     * @return the password {@link CouchbaseConfigurations#SCOPE}
     */
    public Optional<String> getScope() {
        return Optional.ofNullable(scope);
    }

    /**
     * Returns the scope {@link CouchbaseConfigurations#SCOPE}
     *
     * @return the password {@link CouchbaseConfigurations#SCOPE}
     */
    public Optional<String> getCollection() {
        return Optional.ofNullable(collection);
    }

    /**
     * Returns the scope {@link CouchbaseConfigurations#COLLECTIONS}
     *
     * @return the password {@link CouchbaseConfigurations#COLLECTIONS}
     */
    public List<String> getCollections() {
        if (collections == null || collections.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(collections);
    }

    /**
     * Returns the scope {@link CouchbaseConfigurations#INDEX}
     *
     * @return the password {@link CouchbaseConfigurations#INDEX}
     */
    public String getIndex() {
        return index;
    }


    /**
     * Create a new {@link Cluster} instance using {@link CouchbaseSettings#getHost()}
     * {@link  CouchbaseSettings#getUser()} {@link CouchbaseSettings#getPassword()}
     *
     * @return a {@link Cluster} instance
     */
    public Cluster getCluster() {
        return Cluster.connect(host, user, password);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CouchbaseSettings that = (CouchbaseSettings) o;
        return Objects.equals(host, that.host) && Objects.equals(user, that.user)
                && Objects.equals(password, that.password) && Objects.equals(scope, that.scope)
                && Objects.equals(index, that.index) && Objects.equals(collection, that.collection)
                && Objects.equals(collections, that.collections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, user, password, scope, index, collection, collections);
    }

    @Override
    public String toString() {
        return "CouchbaseSettings{" +
                "host='" + host + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", scope='" + scope + '\'' +
                ", index='" + index + '\'' +
                ", collection='" + collection + '\'' +
                ", collections=" + collections +
                '}';
    }
}
