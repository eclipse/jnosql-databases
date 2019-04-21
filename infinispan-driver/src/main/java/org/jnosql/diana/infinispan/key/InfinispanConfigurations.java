package org.jnosql.diana.infinispan.key;

import java.util.function.Supplier;

public enum InfinispanConfigurations implements Supplier<String> {

HOST("infinispan.host"), CONFIG("infinispan.config");

    private final String configuration;

    InfinispanConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
