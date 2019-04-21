package org.jnosql.diana.couchbase;

import java.util.function.Supplier;

public enum CouchbaseConfigurations implements Supplier<String> {

    HOST("couchbase.host"),
    USER("couchbase.user"),
    PASSWORD("couchbase.password");

    private final String configuration;

    CouchbaseConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
