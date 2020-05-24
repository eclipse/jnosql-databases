/*
 *  Copyright (c) 2020 Otávio Santana and others
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Apache License v2.0 which accompanies this distribution.
 *   The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *   and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 *   You may elect to redistribute this code under either of these licenses.
 *
 *   Contributors:
 *
 *   Otavio Santana
 */

package jakarta.nosql.communication.tck.driver.keyvalue;


import jakarta.nosql.ServiceLoaderProvider;
import jakarta.nosql.Value;
import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import jakarta.nosql.keyvalue.KeyValueEntity;
import org.eclipse.jnosql.diana.redis.keyvalue.RedisBucketManagerFactorySupplier;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class BucketManagerTest {


    private User userOtavio = new User("otavio");
    private KeyValueEntity keyValueOtavio = KeyValueEntity.of("otavio", Value.of(userOtavio));

    private User userSoro = new User("soro");
    private KeyValueEntity keyValueSoro = KeyValueEntity.of("soro", Value.of(userSoro));

    @BeforeEach
    public void init() {

    }


    @Test
    public void shouldPutValue() {
        final BucketManager manager = getBucketManager();
        manager.put("otavio", userOtavio);
        Optional<Value> otavio = manager.get("otavio");
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));
    }

    @Test
    public void shouldPutKeyValue() {
        final BucketManager manager = getBucketManager();
        manager.put(keyValueOtavio);
        Optional<Value> otavio = manager.get("otavio");
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));
    }

    @Test
    public void shouldPutIterableKeyValue() {
        final BucketManager manager = getBucketManager();
        manager.put(asList(keyValueSoro, keyValueOtavio));
        Optional<Value> otavio = manager.get("otavio");
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));

        Optional<Value> soro = manager.get("soro");
        assertTrue(soro.isPresent());
        assertEquals(userSoro, soro.get().get(User.class));
    }

    @Test
    public void shouldMultiGet() {
        final BucketManager manager = getBucketManager();
        User user = new User("otavio");
        KeyValueEntity keyValue = KeyValueEntity.of("otavio", Value.of(user));
        manager.put(keyValue);
        assertNotNull(manager.get("otavio"));
    }

    @Test
    public void shouldRemoveKey() {
        final BucketManager manager = getBucketManager();
        manager.put(keyValueOtavio);
        assertTrue(manager.get("otavio").isPresent());
        manager.delete("otavio");
        assertFalse(manager.get("otavio").isPresent());
    }

    @Test
    public void shouldRemoveMultiKey() {
        final BucketManager manager = getBucketManager();
        manager.put(asList(keyValueSoro, keyValueOtavio));
        List<String> keys = asList("otavio", "soro");
        Iterable<Value> values = manager.get(keys);
        assertThat(StreamSupport.stream(values.spliterator(), false).map(value -> value.get(User.class))
                        .collect(Collectors.toList()),
                Matchers.containsInAnyOrder(userOtavio, userSoro));
        manager.delete(keys);
        assertEquals(0L, StreamSupport.stream(manager.get(keys).spliterator(), false).count());
    }

    @AfterEach
    public void remove() {
        final Optional<BucketManagerSupplier> supplier = getSupplier();
        if (!supplier.isPresent()) {
            final BucketManager manager = getBucketManager();
            manager.delete(Arrays.asList("otavio", "soro"));
        }
    }


    private BucketManager getBucketManager() {
        final Optional<BucketManagerSupplier> supplier = getSupplier();
        assumeTrue(supplier.isPresent(), "Checking a BucketManagerSupplier implementation");

        final BucketManagerSupplier bucketManagerSupplier = supplier.get();
        return bucketManagerSupplier.get();
    }

    private Optional<BucketManagerSupplier> getSupplier() {
        return ServiceLoaderProvider
                .getSupplierStream(BucketManagerSupplier.class)
                .map(BucketManagerSupplier.class::cast)
                .findFirst();
    }

}
