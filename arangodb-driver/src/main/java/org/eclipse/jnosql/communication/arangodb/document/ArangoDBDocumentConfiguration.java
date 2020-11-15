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

package org.eclipse.jnosql.communication.arangodb.document;

import com.arangodb.ArangoDB;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.arangodb.ArangoDBConfiguration;
import org.eclipse.jnosql.communication.arangodb.ArangoDBConfigurations;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The implementation of {@link DocumentConfiguration}
 * that returns {@link ArangoDBDocumentCollectionManagerFactory}.
 * It tries to read the configuration properties from diana-arangodb.properties file.
 *
 * @see ArangoDBConfiguration
 * @see ArangoDBConfigurations
 *
 */
public final class ArangoDBDocumentConfiguration extends ArangoDBConfiguration
        implements DocumentConfiguration {

    @Override
    public ArangoDBDocumentCollectionManagerFactory get() throws UnsupportedOperationException {
        Map<String, String> configuration = ConfigurationReader.from(ArangoDBConfigurations.FILE_CONFIGURATION.get());
        SettingsBuilder builder = Settings.builder();
        configuration.entrySet().stream().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }

    @Override
    public ArangoDBDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");

        ArangoDB arangoDB = getArangoDB(settings);
        return new ArangoDBDocumentCollectionManagerFactory(arangoDB);
    }

}
