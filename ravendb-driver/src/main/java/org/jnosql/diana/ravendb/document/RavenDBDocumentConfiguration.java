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

package org.jnosql.diana.ravendb.document;

import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.document.DocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


/**
 * The RavenDB implementation to both {@link DocumentConfiguration}
 * that returns  {@link RavenDBDocumentCollectionManagerFactory}
 * It tries to read the diana-ravendb.properties file whose has the following properties
 * <p>ravendb-server-host-: as prefix to add host client, eg: ravendb-server-host-1=host1, ravendb-server-host-2= host2</p>
 */
public class RavenDBDocumentConfiguration implements DocumentConfiguration<RavenDBDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-ravendb.properties";

    private List<String> hosts = new ArrayList<>();

    @Override
    public RavenDBDocumentCollectionManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(configuration);
    }

    @Override
    public RavenDBDocumentCollectionManagerFactory get(Settings settings) {
        requireNonNull(settings, "configurations is required");
        Map<String, String> configurations = new HashMap<>();
        settings.forEach((key, value) -> configurations.put(key, value.toString()));
        return get(configurations);
    }


    private RavenDBDocumentCollectionManagerFactory get(Map<String, String> configurations) throws NullPointerException {
        requireNonNull(configurations, "configurations is required");
        List<String> servers = configurations.keySet().stream().filter(s -> s.startsWith("ravendb-server-host-"))
                .map(configurations::get).collect(Collectors.toList());
        if (servers.isEmpty()) {
            return new RavenDBDocumentCollectionManagerFactory(new MongoClient());
        }

        return new RavenDBDocumentCollectionManagerFactory(new MongoClient(servers));
    }
}
