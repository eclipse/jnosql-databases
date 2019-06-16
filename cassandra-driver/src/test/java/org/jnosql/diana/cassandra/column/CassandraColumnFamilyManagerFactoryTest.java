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

import com.datastax.driver.core.Cluster;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.column.ColumnFamilyManager;
import jakarta.nosql.column.ColumnFamilyManagerAsync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraColumnFamilyManagerFactoryTest {

    private CassandraColumnFamilyManagerFactory subject;


    @BeforeEach
    public void setUp() {
        Settings settings = ManagerFactorySupplier.INSTANCE.getSettings();
        SettingsBuilder builder = Settings.builder();
        builder.put("cassandra.host.1", settings.get("cassandra.host-1").get().toString());
        builder.put("cassandra.port", settings.get("cassandra.port").get().toString());
        builder.put("cassandra.query.1", " CREATE KEYSPACE IF NOT EXISTS newKeySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        subject = cassandraConfiguration.get(builder.build());
    }

    @Test
    public void shouldReturnErrorWhenSettingsIsNull() {
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        assertThrows(NullPointerException.class, () -> {
            cassandraConfiguration.get(null);
        });

        assertThrows(NullPointerException.class, () -> {
            cassandraConfiguration.get(null);
        });
    }

    @Test
    public void shouldReturnEntityManager() {
        ColumnFamilyManager columnEntityManager = subject.get(org.jnosql.diana.cassandra.column.Constants.KEY_SPACE);
        assertNotNull(columnEntityManager);
    }

    @Test
    public void shouldReturnEntityManagerAsync() {
        ColumnFamilyManagerAsync columnEntityManager = subject.getAsync(org.jnosql.diana.cassandra.column.Constants.KEY_SPACE);
        assertNotNull(columnEntityManager);
    }

    @Test
    public void shouldCloseNode() {
        subject.close();
        CassandraColumnFamilyManagerFactory cassandraColumnFamilyManagerFactory = CassandraColumnFamilyManagerFactory.class.cast(subject);
        Cluster cluster = cassandraColumnFamilyManagerFactory.getCluster();
        assertTrue(cluster.isClosed());
    }

}