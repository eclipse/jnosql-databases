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
package org.jnosql.diana.orientdb.document;


import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Map;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * The orientDB implementation of {@link UnaryDocumentConfiguration} that returns
 * {@link OrientDBDocumentCollectionManagerFactory}. It tries to read diana-arangodb.properties file.
 * <p>orientdb-server-host: the host</p>
 * <p>orientdb-server-user: the user</p>
 * <p>orientdb-server-password: the password</p>
 * <p>orientdb-server-storageType: the storage type</p>
 */
public class OrientDBDocumentConfiguration implements UnaryDocumentConfiguration<OrientDBDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-orientdb.properties";

    private static final Logger LOGGER = Logger.getLogger(OrientDBDocumentConfiguration.class.getName());

    private String host;

    private String user;

    private String password;

    private String storageType;

    public OrientDBDocumentConfiguration() {
        Map<String, String> properties = ConfigurationReader.from(FILE_CONFIGURATION);
        this.host = properties.get("orientdb-server-host");
        this.user = properties.get("orientdb-server-user");
        this.password = properties.get("orientdb-server-password");
        this.storageType = properties.get("orientdb-server-storageType");
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    @Override
    public OrientDBDocumentCollectionManagerFactory get() {
        return new OrientDBDocumentCollectionManagerFactory(host, user, password, storageType);
    }

    @Override
    public OrientDBDocumentCollectionManagerFactory get(Settings settings) throws NullPointerException {
        return getOrientDBDocumentCollectionManagerFactory(settings);
    }

    @Override
    public OrientDBDocumentCollectionManagerFactory getAsync() throws UnsupportedOperationException {
        return new OrientDBDocumentCollectionManagerFactory(host, user, password, storageType);
    }

    @Override
    public OrientDBDocumentCollectionManagerFactory getAsync(Settings settings) throws NullPointerException {
        return getOrientDBDocumentCollectionManagerFactory(settings);
    }


    private OrientDBDocumentCollectionManagerFactory getOrientDBDocumentCollectionManagerFactory(Settings settings) {
        requireNonNull(settings, "settings is required");

        String host = getValue(settings, "orientdb-server-host");
        String user = getValue(settings, "orientdb-server-user");
        String password = getValue(settings, "orientdb-server-password");
        String storageType = getValue(settings, "orientdb-server-storageType");
        return new OrientDBDocumentCollectionManagerFactory(host, user, password, storageType);
    }

    private String getValue(Settings settings, String key) {
        return ofNullable(settings.get(key)).map(Object::toString).orElse(null);
    }
}
