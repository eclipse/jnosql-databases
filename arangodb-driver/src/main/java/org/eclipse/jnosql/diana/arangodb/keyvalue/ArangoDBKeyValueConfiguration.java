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
package org.eclipse.jnosql.diana.arangodb.keyvalue;


import com.arangodb.ArangoDB;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.keyvalue.KeyValueConfiguration;
import org.eclipse.jnosql.diana.arangodb.ArangoDBConfiguration;
import org.eclipse.jnosql.diana.arangodb.ArangoDBConfigurations;
import org.eclipse.jnosql.diana.driver.ConfigurationReader;

import java.util.Map;

/**
 * The ArangoDB implementation to {@link KeyValueConfiguration}
 * It tries to read the configuration properties from diana-arangodb.properties file.
 *
 * @see ArangoDBConfigurations
 */
public final class ArangoDBKeyValueConfiguration extends ArangoDBConfiguration
        implements KeyValueConfiguration {

    @Override
    public ArangoDBBucketManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(ArangoDBConfigurations.FILE_CONFIGURATION.get());
        SettingsBuilder builder = Settings.builder();
        configuration.entrySet().stream().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }

    @Override
    public ArangoDBBucketManagerFactory get(Settings settings) {
        ArangoDB arangoDB = getArangoDB(settings);
        return new ArangoDBBucketManagerFactory(arangoDB);
    }
}
