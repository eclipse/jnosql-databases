/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package org.jnosql.diana.cassandra.column;

import com.datastax.driver.core.Cluster;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsync;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class CassandraColumnFamilyManagerFactoryTest {

    private CassandraDocumentEntityManagerFactory subject;

    @Before
    public void setUp() throws InterruptedException, IOException, TTransportException {
        Map<String, String> configurations = new HashMap<>();
        configurations.put("cassandra-hoster-1", "localhost");
        configurations.put("cassandra-port", "9142");
        configurations.put("cassandra-query-1", " CREATE KEYSPACE IF NOT EXISTS newKeySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
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

    @After
    public void end(){
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
}