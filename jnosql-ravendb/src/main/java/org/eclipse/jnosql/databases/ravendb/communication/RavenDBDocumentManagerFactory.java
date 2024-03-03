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

package org.eclipse.jnosql.databases.ravendb.communication;

import net.ravendb.client.documents.DocumentStore;
import org.eclipse.jnosql.communication.semistructured.DatabaseManagerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * The RavenDB implementation to {@link DatabaseManagerFactory}
 */
public class RavenDBDocumentManagerFactory implements DatabaseManagerFactory {

    private final String[] hosts;

    RavenDBDocumentManagerFactory(String[] hosts) {
        this.hosts = hosts;
    }

    @Override
    public RavenDBDocumentManager apply(String database) {
        Objects.requireNonNull(database, "database is required");
        DocumentStore documentStore = new DocumentStore(hosts, database);
        return new RavenDBDocumentManager(documentStore, database);
    }


    @Override
    public void close() {

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RavenDBDocumentManagerFactory{");
        sb.append("hosts=").append(Arrays.toString(hosts));
        sb.append('}');
        return sb.toString();
    }
}
