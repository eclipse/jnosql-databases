package org.jnosql.diana.cassandra.column;

import org.jnosql.diana.driver.ConfigurationReader;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Map;
import java.util.function.Supplier;

import static org.jnosql.diana.cassandra.column.CassandraConfiguration.CASSANDRA_FILE_CONFIGURATION;

public enum CassandraConfigurationSupplier implements Supplier<CassandraConfiguration> {
    INSTANCE;

    private final GenericContainer cassandra =
            new GenericContainer("cassandra:latest")
                    .withExposedPorts(9042)
                    .waitingFor(Wait.defaultWaitStrategy());

    {
        cassandra.start();
    }

    @Override
    public CassandraConfiguration get() {
        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        configuration.put("cassandra-host-1", "localhost");
        configuration.put("cassandra-port", "9142");

        return null;
    }
}
