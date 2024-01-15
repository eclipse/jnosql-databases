package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public enum Database {

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

}
