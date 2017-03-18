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


import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.jnosql.diana.api.column.ColumnFamilyManagerFactory;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class CassandraConfigurationTest {

    @Test
    public void shoudlCreateDocumentEntityManagerFactory() throws InterruptedException, IOException, TTransportException {
        Map<String, String> configurations = new HashMap<>();
        configurations.put("cassandra-hoster-1", "localhost");
        configurations.put("cassandra-port", "9142");
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
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

    @After
    public void end(){
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
}
