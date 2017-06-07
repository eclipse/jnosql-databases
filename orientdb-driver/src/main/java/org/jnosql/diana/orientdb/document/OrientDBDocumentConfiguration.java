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


import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Map;
import java.util.logging.Logger;

/**
 * The orientDB implementation of {@link UnaryDocumentConfiguration} that returns
 * {@link OrientDBDocumentCollectionManagerFactory}. It tries to read diana-arangodb.properties file.
 * <p>mongodb-server-host: the host</p>
 * <p>mongodb-server-user: the user</p>
 * <p>mongodb-server-password: the password</p>
 * <p>mongodb-server-storageType: the storage type</p>
 */
public class OrientDBDocumentConfiguration implements UnaryDocumentConfiguration<OrientDBDocumentCollectionManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-arangodb.properties";

    private static final Logger LOGGER = Logger.getLogger(OrientDBDocumentConfiguration.class.getName());

    private String host;

    private String user;

    private String password;

    private String storageType;

    public OrientDBDocumentConfiguration() {
        Map<String, String> properties = ConfigurationReader.from(FILE_CONFIGURATION);
        this.host = properties.get("mongodb-server-host");
        this.user = properties.get("mongodb-server-user");
        this.password = properties.get("mongodb-server-password");
        this.storageType = properties.get("mongodb-server-storageType");
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
    public OrientDBDocumentCollectionManagerFactory getAsync() throws UnsupportedOperationException {
        return new OrientDBDocumentCollectionManagerFactory(host, user, password, storageType);
    }
}
