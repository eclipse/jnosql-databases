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
package org.eclipse.jnosql.communication.couchbase.document;


import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.couchbase.CouchbaseConfiguration;
import org.eclipse.jnosql.communication.couchbase.CouchbaseConfigurations;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation of {@link DocumentConfiguration}  that returns
 * {@link CouchbaseDocumentCollectionManagerFactory}.
 * @see CouchbaseConfigurations
 */
public class CouchbaseDocumentConfiguration extends CouchbaseConfiguration
        implements DocumentConfiguration {

    @Override
    public CouchbaseDocumentCollectionManagerFactory get() throws UnsupportedOperationException {
        return new CouchbaseDocumentCollectionManagerFactory(toCouchbaseSettings());
    }

    @Override
    public CouchbaseDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");

        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        configuration.update(settings);
        return new CouchbaseDocumentCollectionManagerFactory(configuration.toCouchbaseSettings());
    }

}
