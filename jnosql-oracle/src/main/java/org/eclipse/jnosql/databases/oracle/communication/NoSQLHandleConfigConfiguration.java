package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import org.eclipse.jnosql.communication.Configurations;
import org.eclipse.jnosql.communication.Settings;

import java.util.List;
import java.util.function.Function;

enum NoSQLHandleConfigConfiguration implements Function<Settings, NoSQLHandle> {

    INSTANCE;

    private static final String DEFAULT_HOST = "http://localhost:8080";
    private static final String DEFAULT_TABLE_READ_LIMITS = "http://localhost:8080";
    private static final String DEFAULT_TABLE_WRITE_LIMITS = "http://localhost:8080";
    private static final String DEFAULT_TABLE_STORAGE_GB = "http://localhost:8080";
    private static final String DEFAULT_TABLE_WAIT_MILLIS = "http://localhost:8080";
    private static final String DEFAULT_TABLE_DELAY_MILLIS = "http://localhost:8080";
    @Override
    public NoSQLHandle apply(Settings settings) {
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
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }
}
