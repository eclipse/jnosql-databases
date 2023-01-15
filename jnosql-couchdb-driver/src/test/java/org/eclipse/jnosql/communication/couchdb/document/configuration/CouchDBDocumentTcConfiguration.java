/*
 *
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
 *
 */
package org.eclipse.jnosql.communication.couchdb.document.configuration;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.Settings.SettingsBuilder;
import org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations;
import org.eclipse.jnosql.communication.couchdb.document.CouchDBDocumentConfiguration;
import org.eclipse.jnosql.communication.couchdb.document.CouchDBDocumentManagerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.function.Supplier;

public enum CouchDBDocumentTcConfiguration implements Supplier<CouchDBDocumentManagerFactory> {

    INSTANCE;


    private GenericContainer couchDB = new CouchDBContainer();

    {
        couchDB.start();
    }

    @Override
    public CouchDBDocumentManagerFactory get() {
        CouchDBDocumentConfiguration configuration = new CouchDBDocumentConfiguration();
        SettingsBuilder builder = Settings.builder();
        builder.put(CouchDBConfigurations.PORT, couchDB.getFirstMappedPort());
        builder.put(CouchDBConfigurations.USER, "admin");
        builder.put(CouchDBConfigurations.PASSWORD, "password");
        return configuration.apply(builder.build());
    }

}
