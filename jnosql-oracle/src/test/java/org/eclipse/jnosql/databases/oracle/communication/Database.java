package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import org.eclipse.jnosql.communication.Configurations;
import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.KeyValueConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.function.Supplier;

public enum Database implements Supplier<BucketManagerFactory> {

    INSTANCE;
    private final GenericContainer<?> container = new GenericContainer<>
            (DockerImageName.parse("ghcr.io/oracle/nosql:latest-ce"))
            .withExposedPorts(8080);

    {
        container.start();
    }

    NoSQLHandle getNoSQLHandle() {
        String address = container.getHost();
        Integer port = container.getFirstMappedPort();
        //NoSQLHandleConfig config = new NoSQLHandleConfig("http://" + System.getenv("NOSQL_ENDPOINT") + ":" + System.getenv("NOSQL_PORT"));
        System.out.println("Connecting to http://" + address + ":" + port);
        NoSQLHandleConfig config = new NoSQLHandleConfig("http://" + address + ":" + port);
        config.setAuthorizationProvider(new StoreAccessTokenProvider());
        return NoSQLHandleFactory.createNoSQLHandle(config) ;
    }

    @Override
    public BucketManagerFactory get() {
        KeyValueConfiguration configuration = new OracleKeyValueConfiguration();
        Settings settings = Settings.builder()
                .put(OracleConfigurations.HOST, "http://" + container.getHost() + ":" + container.getFirstMappedPort())
                .build();
        return configuration.apply(settings);
    }
}
