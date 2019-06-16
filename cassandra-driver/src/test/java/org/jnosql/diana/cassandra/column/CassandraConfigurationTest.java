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

package org.jnosql.diana.cassandra.column;


import jakarta.nosql.Settings;
import jakarta.nosql.column.ColumnFamilyManagerFactory;
import jakarta.nosql.column.UnaryColumnConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CassandraConfigurationTest {



    @Test
    public void shouldCreateDocumentEntityManagerFactoryFromSettings() {
        Settings settings = ManagerFactorySupplier.INSTANCE.getSettings();
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        ColumnFamilyManagerFactory entityManagerFactory = cassandraConfiguration.get(settings);
        assertNotNull(entityManagerFactory);
    }


    @Test
    public void shouldCreateDocumentEntityManagerFactoryFromFile() {
        Settings settings = ManagerFactorySupplier.INSTANCE.getSettings();
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        ColumnFamilyManagerFactory entityManagerFactory = cassandraConfiguration.get(settings);
        assertNotNull(entityManagerFactory);
    }

    @Test
    public void shouldCreateConfiguration() {
        UnaryColumnConfiguration<?> configuration = UnaryColumnConfiguration.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue( configuration instanceof CassandraConfiguration);
    }

    @Test
    public void shouldCreateConfigurationQuery() {
        CassandraConfiguration configuration = UnaryColumnConfiguration.getConfiguration(CassandraConfiguration.class);
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue( configuration instanceof CassandraConfiguration);
    }
}
