package org.jnosql.diana.riak.key;

import jakarta.nosql.CommunicationException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RiakContainer extends GenericContainer {

    public RiakContainer() {
        super("basho/riak-ts");
        addFixedExposedPort(8087, 8087);
        addFixedExposedPort(8098, 8098);
        waitStrategy();
    }

    private void waitStrategy() {
        this.waitStrategy = new WaitStrategy() {
            @Override
            public void waitUntilReady(WaitStrategyTarget target) {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new CommunicationException("Error to wait", e);
                }
            }

            @Override
            public WaitStrategy withStartupTimeout(Duration startupTimeout) {
                return this;
            }
        };
    }
}
