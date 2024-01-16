/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.KeyValueConfiguration;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The Oracle implementation of {@link KeyValueConfiguration} that returns
 * {@link OracleBucketManagerFactory}.
 *
 */
public class OracleKeyValueConfiguration implements KeyValueConfiguration {
    @Override
    public BucketManagerFactory apply(Settings settings) {
        requireNonNull(settings, "settings is required");
        String host = settings.get(List.of(OracleConfigurations.HOST.get(), Configurations.HOST.get()))
                .map(Object::toString).orElse("http://localhost:8080");
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
        var nosql =  NoSQLHandleFactory.createNoSQLHandle(config);
        return new OracleBucketManagerFactory(nosql);
    }
}
