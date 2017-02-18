/*
 * Copyright 2017 Otavio Santana and others
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
