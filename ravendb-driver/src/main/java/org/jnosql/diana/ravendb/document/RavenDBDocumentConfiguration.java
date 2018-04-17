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

import net.ravendb.client.documents.IDocumentStore;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.document.DocumentConfiguration;
import org.jnosql.diana.api.document.DocumentConfigurationAsync;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


/**
 * The MongoDB implementation to both {@link DocumentConfiguration} and {@link DocumentConfigurationAsync}
 * that returns  {@link MongoDBDocumentCollectionManagerFactory} {@link MongoDBDocumentCollectionManagerAsyncFactory}.
 * It tries to read the diana-mongodb.properties file whose has the following properties
 * <p>mongodb-server-host-: as prefix to add host client, eg: mongodb-server-host-1=host1, mongodb-server-host-2= host2</p>
 */
public class RavenDBDocumentConfiguration implements DocumentConfiguration<MongoDBDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-ravendb.properties";

    static final int DEFAULT_PORT = 27017;


    @Override
    public MongoDBDocumentCollectionManagerFactory get() {
        return null;
    }

    @Override
    public MongoDBDocumentCollectionManagerFactory get(Settings settings) {
        return null;
    }
}
