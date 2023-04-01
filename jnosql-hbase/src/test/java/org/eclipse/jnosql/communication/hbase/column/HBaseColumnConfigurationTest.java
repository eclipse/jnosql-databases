/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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

package org.eclipse.jnosql.communication.hbase.column;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.column.ColumnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION_MATCHES;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfSystemProperty(named = INTEGRATION, matches = INTEGRATION_MATCHES)
public class HBaseColumnConfigurationTest {


    @Test
    public void shouldCreatesColumnManagerFactory() {
        ColumnConfiguration configuration = new HBaseColumnConfiguration();
        assertNotNull(configuration.apply(Settings.builder().build()));
    }

    @Test
    public void shouldCreatesColumnManagerFactoryFromConfiguration() {
        ColumnConfiguration configuration = new HBaseColumnConfiguration();
        assertNotNull(configuration.apply(Settings.builder().build()));
    }

    @Test
    public void shouldReturnErrorCreatesColumnManagerFactory() {
        assertThrows(NullPointerException.class, () -> new HBaseColumnConfiguration(null));
    }
}