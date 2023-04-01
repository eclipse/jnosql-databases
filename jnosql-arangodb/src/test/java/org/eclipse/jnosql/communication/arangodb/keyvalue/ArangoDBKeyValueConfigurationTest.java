/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.arangodb.keyvalue;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.arangodb.document.ArangoDBDocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.KeyValueConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ArangoDBKeyValueConfigurationTest {

    @Test
    public void shouldCreateDocumentManagerFactory() {
        ArangoDBKeyValueConfiguration configuration = new ArangoDBKeyValueConfiguration();
        configuration.addHost("localhost", 8529);
        BucketManagerFactory managerFactory = configuration.apply(Settings.builder().build());
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldReturnFromConfiguration() {
        ArangoDBKeyValueConfiguration configuration = new ArangoDBKeyValueConfiguration();
        Assertions.assertNotNull(configuration);
        assertThat(configuration).isInstanceOf(ArangoDBKeyValueConfiguration.class);
    }

    @Test
    public void shouldReturnFromConfigurationQuery() {
        ArangoDBKeyValueConfiguration configuration = KeyValueConfiguration
                .getConfiguration(ArangoDBKeyValueConfiguration.class);
        Assertions.assertNotNull(configuration);
        assertThat(configuration).isInstanceOf(ArangoDBKeyValueConfiguration.class);
    }
}
