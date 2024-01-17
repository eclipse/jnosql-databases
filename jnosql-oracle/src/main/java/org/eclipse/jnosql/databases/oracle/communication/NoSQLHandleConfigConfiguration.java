/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import org.eclipse.jnosql.communication.Configurations;
import org.eclipse.jnosql.communication.Settings;

import java.util.List;
import java.util.function.Function;

enum NoSQLHandleConfigConfiguration implements Function<Settings, NoSQLHandleConfiguration> {

    INSTANCE;

    private static final String DEFAULT_HOST = "http://localhost:8080";
    private static final int DEFAULT_TABLE_READ_LIMITS = 25;
    private static final int DEFAULT_TABLE_WRITE_LIMITS = 25;
    private static final int DEFAULT_TABLE_STORAGE_GB = 25;
    private static final int DEFAULT_TABLE_WAIT_MILLIS = 120_000;
    private static final int DEFAULT_TABLE_DELAY_MILLIS = 500;
    @Override
    public NoSQLHandleConfiguration apply(Settings settings) {
        String host = settings.get(List.of(OracleConfigurations.HOST.get(), Configurations.HOST.get()))
                .map(Object::toString).orElse(DEFAULT_HOST);
        String user = settings.get(List.of(OracleConfigurations.USER.get(), Configurations.USER.get()))
                .map(Object::toString).orElse(null);
        String password = settings.get(List.of(OracleConfigurations.PASSWORD.get(), Configurations.PASSWORD.get()))
                .map(Object::toString).orElse(null);

        NoSQLHandleConfig config = new NoSQLHandleConfig(host);
        if (user != null && password != null) {
            config.setAuthorizationProvider(new StoreAccessTokenProvider(user, password.toCharArray()));
        } else {
            config.setAuthorizationProvider(new StoreAccessTokenProvider());
        }
        int readLimit = settings.getOrDefault(OracleConfigurations.TABLE_READ_LIMITS, DEFAULT_TABLE_READ_LIMITS);
        int writeLimit = settings.getOrDefault(OracleConfigurations.TABLE_WRITE_LIMITS, DEFAULT_TABLE_WRITE_LIMITS);
        int storageGB = settings.getOrDefault(OracleConfigurations.TABLE_STORAGE_GB, DEFAULT_TABLE_STORAGE_GB);
        int waitMillis = settings.getOrDefault(OracleConfigurations.TABLE_WAIT_MILLIS, DEFAULT_TABLE_WAIT_MILLIS);
        int delayMillis = settings.getOrDefault(OracleConfigurations.TABLE_DELAY_MILLIS, DEFAULT_TABLE_DELAY_MILLIS);
        var tableLimits = new TableCreationConfiguration(readLimit, writeLimit, storageGB, waitMillis, delayMillis);
        return new NoSQLHandleConfiguration(NoSQLHandleFactory.createNoSQLHandle(config), tableLimits);
    }
}
