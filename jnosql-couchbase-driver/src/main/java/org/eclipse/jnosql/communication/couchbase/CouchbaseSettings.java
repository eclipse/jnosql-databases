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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Couchbase Configuration.
 */
public final class CouchbaseSettings {

    private final String host;

    private final String user;

    private final String password;

    private final String scope;

    private final List<String> collections;

    private final String index;

    CouchbaseSettings(String host, String user, String password, String scope,
                      List<String> collections, String index) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.scope = scope;
        this.collections = collections;
        this.index = index;
    }

    public String getHost() {
        return host;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getScope() {
        return scope;
    }

    public List<String> getCollections() {
        if (collections == null || collections.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(collections);
    }

    public String getIndex() {
        return index;
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
                && Objects.equals(collections, that.collections) && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, user, password, scope, collections, index);
    }

    @Override
    public String toString() {
        return "CouchbaseSettings{" +
                "host='" + host + '\'' +
                ", user='" + user + '\'' +
                ", password='" + "****" + '\'' +
                ", scope='" + scope + '\'' +
                ", collections=" + collections +
                ", index='" + index + '\'' +
                '}';
    }
}
