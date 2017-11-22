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
import com.datastax.driver.core.Session;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsyncFactory;
import org.jnosql.diana.api.column.ColumnFamilyManagerFactory;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * The Cassandra implementation to {@link ColumnFamilyManagerFactory}
 */
public class CassandraColumnFamilyManagerFactory implements ColumnFamilyManagerFactory<CassandraColumnFamilyManager>
        , ColumnFamilyManagerAsyncFactory<CassandraColumnFamilyManagerAsync> {

    private final Cluster cluster;

    private final Executor executor;

    CassandraColumnFamilyManagerFactory(final Cluster cluster, List<String> queries, Executor executor) {
        this.cluster = cluster;
        this.executor = executor;
        runIniticialQuery(queries);
    }

    public void runIniticialQuery(List<String> queries) {
        Session session = cluster.connect();
        queries.forEach(session::execute);
        session.close();
    }

    @Override
    public CassandraColumnFamilyManager get(String database) {
        return new DefaultCassandraColumnFamilyManager(cluster.connect(database), executor, database);
    }

    @Override
    public CassandraColumnFamilyManagerAsync getAsync(String database) throws UnsupportedOperationException, NullPointerException {
        return new DefaultCassandraColumnFamilyManagerAsync(cluster.connect(database), executor, database);
    }

    @Override
    public void close() {
        cluster.close();
    }

    Cluster getCluster() {
        return cluster;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraColumnFamilyManagerFactory{");
        sb.append("cluster=").append(cluster);
        sb.append(", executor=").append(executor);
        sb.append('}');
        return sb.toString();
    }
}
