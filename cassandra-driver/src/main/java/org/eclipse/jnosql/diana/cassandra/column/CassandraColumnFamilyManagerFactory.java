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

package org.eclipse.jnosql.diana.cassandra.column;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import jakarta.nosql.column.ColumnFamilyManagerFactory;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * The Cassandra implementation to {@link ColumnFamilyManagerFactory}
 */
public class CassandraColumnFamilyManagerFactory implements ColumnFamilyManagerFactory {

    private final CqlSessionBuilder sessionBuilder;

    private final Executor executor;

    CassandraColumnFamilyManagerFactory(final CqlSessionBuilder sessionBuilder, List<String> queries, Executor executor) {
        this.sessionBuilder = sessionBuilder;
        this.executor = executor;
        load(queries);
    }

    void load(List<String> queries) {
        final CqlSession session = sessionBuilder.build();
        queries.forEach(session::execute);
        session.close();
    }

    @Override
    public CassandraColumnFamilyManager get(String database) {
        return new DefaultCassandraColumnFamilyManager(sessionBuilder.build(), executor, database);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraColumnFamilyManagerFactory{");
        sb.append("cluster=").append(sessionBuilder);
        sb.append(", executor=").append(executor);
        sb.append('}');
        return sb.toString();
    }
}
