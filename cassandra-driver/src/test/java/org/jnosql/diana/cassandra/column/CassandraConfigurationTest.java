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


import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.jnosql.diana.api.column.ColumnFamilyManagerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class CassandraConfigurationTest {

    @BeforeClass
    public static void before() throws InterruptedException, IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    @AfterClass
    public static void end(){
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Test
    public void shoudlCreateDocumentEntityManagerFactory() throws InterruptedException, IOException, TTransportException {
        Map<String, String> configurations = new HashMap<>();
        configurations.put("cassandra-hoster-1", "localhost");
        configurations.put("cassandra-port", "9142");
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        ColumnFamilyManagerFactory entityManagerFactory = cassandraConfiguration.getManagerFactory(configurations);
        assertNotNull(entityManagerFactory);
    }

    @Test
    public void shoudlCreateDocumentEntityManagerFactoryFromFile() {
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        ColumnFamilyManagerFactory entityManagerFactory = cassandraConfiguration.get();
        assertNotNull(entityManagerFactory);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnNPEWhenMapIsNull() {
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        cassandraConfiguration.getManagerFactory(null);
    }

}
