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

package org.eclipse.jnosql.databases.cassandra.communication;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.eclipse.jnosql.communication.column.ColumnManagerFactory;

import java.util.List;

/**
 * The Cassandra implementation to {@link ColumnManagerFactory}
 */
public class CassandraColumnManagerFactory implements ColumnManagerFactory {

    private final CqlSessionBuilder sessionBuilder;

    CassandraColumnManagerFactory(final CqlSessionBuilder sessionBuilder, List<String> queries) {
        this.sessionBuilder = sessionBuilder;
        load(queries);
    }

    void load(List<String> queries) {
        final CqlSession session = sessionBuilder.build();
        queries.forEach(session::execute);
        session.close();
    }

    @Override
    public CassandraColumnManager apply(String database) {
        return new DefaultCassandraColumnManager(sessionBuilder.build(), database);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraColumnManagerFactory{");
        sb.append("cluster=").append(sessionBuilder);
        sb.append('}');
        return sb.toString();
    }
}
