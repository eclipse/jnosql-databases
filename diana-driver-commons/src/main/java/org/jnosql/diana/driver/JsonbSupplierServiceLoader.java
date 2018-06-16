package org.jnosql.diana.driver;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

final class JsonbSupplierServiceLoader {

    private static final List<JsonbSupplier> LOADERS;

    static final Optional<JsonbSupplier> INSTANCE;

    static {
        ServiceLoader<JsonbSupplier> serviceLoader = ServiceLoader.load(JsonbSupplier.class);
        LOADERS = StreamSupport.stream(serviceLoader.spliterator(), false).collect(toList());
        INSTANCE = LOADERS.stream().findFirst();
    }

    private JsonbSupplierServiceLoader() {
    }

    static JsonbSupplier getInstance() {
        return INSTANCE.orElse(DefaultJsonbSupplier.INSTANCE);
    }
}
