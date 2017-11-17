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
    private static final String SERVER_HOST = "orientdb-server-host";
    private static final String SERVER_USER = "orientdb-server-user";
    private static final String SERVER_PASSWORD = "orientdb-server-password";
    private static final String SERVER_STORAGE_TYPE = "orientdb-server-storageType";

    private String host;

    private String user;

    private String password;

    private String storageType;

    public OrientDBDocumentConfiguration() {
        Map<String, String> properties = ConfigurationReader.from(FILE_CONFIGURATION);
        this.host = properties.get(SERVER_HOST);
        this.user = properties.get(SERVER_USER);
        this.password = properties.get(SERVER_PASSWORD);
        this.storageType = properties.get(SERVER_STORAGE_TYPE);
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

        String host = getValue(settings, SERVER_HOST);
        String user = getValue(settings, SERVER_USER);
        String password = getValue(settings, SERVER_PASSWORD);
        String storageType = getValue(settings, SERVER_STORAGE_TYPE);
        return new OrientDBDocumentCollectionManagerFactory(host, user, password, storageType);
    }

    private String getValue(Settings settings, String key) {
        return ofNullable(settings.get(key)).map(Object::toString).orElse(null);
    }
}
