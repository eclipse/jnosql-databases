package org.jnosql.diana.infinispan.key;

import java.util.function.Supplier;

/**
 * Use {@link InfinispanConfigurations}
 */
@Deprecated
public enum  OldInfinispanConfigurations implements Supplier<String> {

HOST("infinispan-server-"), CONFIG("infinispan-config");

    private final String configuration;

    OldInfinispanConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
