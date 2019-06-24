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
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.CouchbaseCluster;
import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfiguration;
import jakarta.nosql.document.DocumentConfigurationAsync;
import org.jnosql.diana.couchbase.CouchbaseConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation of {@link DocumentConfiguration} and {@link DocumentConfigurationAsync} that returns
 * {@link CouhbaseDocumentCollectionManagerFactory}.
 * <p>couchbase.host-: the prefix to add a new host</p>
 * <p>couchbase.user: the user</p>
 * <p>couchbase.password: the password</p>
 * @see org.jnosql.diana.couchbase.CouchbaseConfigurations
 */
public class CouchbaseDocumentConfiguration extends CouchbaseConfiguration
        implements DocumentConfiguration, DocumentConfigurationAsync {

    @Override
    public CouhbaseDocumentCollectionManagerFactory get() throws UnsupportedOperationException {
        return new CouhbaseDocumentCollectionManagerFactory(CouchbaseCluster.create(nodes), user, password);
    }

    @Override
    public CouhbaseDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");

        Map<String, String> configurations = new HashMap<>();
        settings.forEach((key, value) -> configurations.put(key, value.toString()));

        String user = Optional.ofNullable(getUser(settings)).orElse(this.user);
        String password = Optional.ofNullable(getPassword(settings)).orElse(this.password);
        List<String> hosts = getHosts(settings);

        return new CouhbaseDocumentCollectionManagerFactory(CouchbaseCluster.create(hosts), user, password);
    }

}
