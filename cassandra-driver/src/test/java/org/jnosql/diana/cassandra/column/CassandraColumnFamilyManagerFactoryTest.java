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
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraColumnFamilyManagerFactoryTest {

    private CassandraColumnFamilyManagerFactory subject;


    @BeforeEach
    public void setUp() throws InterruptedException, IOException {
        Settings settings = ManagerFactorySupplier.INSTANCE.getSettings();
        Map<String, String> configurations = new HashMap<>();
        configurations.put("cassandra-host-1", settings.get("cassandra-host-1").toString());
        configurations.put("cassandra-port", settings.get("cassandra-port").toString());
        configurations.put("cassandra-query-1", " CREATE KEYSPACE IF NOT EXISTS newKeySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        subject = cassandraConfiguration.getManagerFactory(configurations);
    }

    @Test
    public void shouldReturnErrorWhenSettingsIsNull() {
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        assertThrows(NullPointerException.class, () -> {
            cassandraConfiguration.get(null);
        });

        assertThrows(NullPointerException.class, () -> {
            cassandraConfiguration.getAsync(null);
        });
    }

    @Test
    public void shouldReturnEntityManager() throws Exception {
        ColumnFamilyManager columnEntityManager = subject.get(org.jnosql.diana.cassandra.column.Constants.KEY_SPACE);
        assertNotNull(columnEntityManager);
    }

    @Test
    public void shouldReturnEntityManagerAsync() throws Exception {
        ColumnFamilyManagerAsync columnEntityManager = subject.getAsync(org.jnosql.diana.cassandra.column.Constants.KEY_SPACE);
        assertNotNull(columnEntityManager);
    }

    @Test
    public void shouldCloseNode() throws Exception {
        subject.close();
        CassandraColumnFamilyManagerFactory cassandraColumnFamilyManagerFactory = CassandraColumnFamilyManagerFactory.class.cast(subject);
        Cluster cluster = cassandraColumnFamilyManagerFactory.getCluster();
        assertTrue(cluster.isClosed());
    }

}