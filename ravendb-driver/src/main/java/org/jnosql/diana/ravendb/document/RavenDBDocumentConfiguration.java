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

import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import org.jnosql.diana.api.document.DocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.Map;

import static java.util.Objects.requireNonNull;


/**
 * The RavenDB implementation to both {@link DocumentConfiguration}
 * that returns  {@link RavenDBDocumentCollectionManagerFactory}
 * It tries to read the diana-ravendb.properties file whose has the following properties
 * <p>ravendb.host-: as prefix to add host client, eg: ravendb.host-1=host1, ravendb.host-2= host2</p>
 */
public class RavenDBDocumentConfiguration implements DocumentConfiguration<RavenDBDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-ravendb.properties";

    @Override
    public RavenDBDocumentCollectionManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(configuration);
    }

    @Override
    public RavenDBDocumentCollectionManagerFactory get(Settings settings) {
        requireNonNull(settings, "configurations is required");

        String[] servers = settings.prefix(Arrays.asList("ravendb.host", Configurations.HOST.get()))
                .stream().map(Object::toString)
                .toArray(String[]::new);
        return new RavenDBDocumentCollectionManagerFactory(servers);
    }

    private RavenDBDocumentCollectionManagerFactory get(Map<String, String> configurations) throws NullPointerException {
        requireNonNull(configurations, "configurations is required");

        SettingsBuilder builder = Settings.builder();
        configurations.entrySet().stream().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }
}
