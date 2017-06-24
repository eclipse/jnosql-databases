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
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsync;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class CassandraColumnFamilyManagerFactoryTest {

    private CassandraDocumentEntityManagerFactory subject;

    @BeforeClass
    public static void before() throws InterruptedException, IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    @AfterClass
    public static void end(){
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Before
    public void setUp() throws InterruptedException, IOException, TTransportException {
        Map<String, String> configurations = new HashMap<>();
        configurations.put("cassandra-hoster-1", "localhost");
        configurations.put("cassandra-port", "9142");
        configurations.put("cassandra-query-1", " CREATE KEYSPACE IF NOT EXISTS newKeySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        subject = cassandraConfiguration.getManagerFactory(configurations);
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
        CassandraDocumentEntityManagerFactory cassandraDocumentEntityManagerFactory = CassandraDocumentEntityManagerFactory.class.cast(subject);
        Cluster cluster = cassandraDocumentEntityManagerFactory.getCluster();
        assertTrue(cluster.isClosed());
    }

}