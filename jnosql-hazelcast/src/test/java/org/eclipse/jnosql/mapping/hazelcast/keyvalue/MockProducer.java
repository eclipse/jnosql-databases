/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.mapping.hazelcast.keyvalue;


import com.hazelcast.query.Predicate;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.interceptor.Interceptor;
import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.hazelcast.keyvalue.HazelcastBucketManager;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class MockProducer implements Supplier<HazelcastBucketManager> {


    @Produces
    @Override
    public HazelcastBucketManager get() {
        HazelcastBucketManager manager = mock(HazelcastBucketManager.class);
        List<Value> people = asList(Value.of(new Person("Poliana", 25)),
                Value.of(new Person("Otavio", 28)));

        when(manager.sql(anyString())).thenReturn(people);
        when(manager.sql(anyString(), any(Map.class))).thenReturn(people);
        when(manager.sql(any(Predicate.class))).thenReturn(people);
        return manager;
    }

}
