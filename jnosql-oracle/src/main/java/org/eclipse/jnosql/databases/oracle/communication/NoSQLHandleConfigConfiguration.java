package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import org.eclipse.jnosql.communication.Configurations;
import org.eclipse.jnosql.communication.Settings;

import java.util.List;
import java.util.function.Function;

enum NoSQLHandleConfigFactory implements Function<Settings, NoSQLHandle> {

    INSTANCE;

    @Override
    public NoSQLHandle apply(Settings settings) {
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
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }
}
