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

package org.jnosql.diana.ravendb.document;

import net.ravendb.client.documents.DocumentStore;
import jakarta.nosql.document.DocumentCollectionManagerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * The RavenDB implementation to {@link DocumentCollectionManagerFactory}
 */
public class RavenDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<RavenDBDocumentCollectionManager> {

    private final String[] hosts;



    RavenDBDocumentCollectionManagerFactory(String[] hosts) {
        this.hosts = hosts;
    }

    @Override
    public RavenDBDocumentCollectionManager get(String database) {
        Objects.requireNonNull(database, "database is required");
        DocumentStore documentStore = new DocumentStore(hosts, database);
        return new RavenDBDocumentCollectionManager(documentStore);
    }


    @Override
    public void close() {

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RavenDBDocumentCollectionManagerFactory{");
        sb.append("hosts=").append(Arrays.toString(hosts));
        sb.append('}');
        return sb.toString();
    }
}
