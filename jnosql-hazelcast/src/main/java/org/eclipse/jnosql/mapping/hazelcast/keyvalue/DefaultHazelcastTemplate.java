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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.hazelcast.keyvalue.HazelcastBucketManager;
import org.eclipse.jnosql.communication.keyvalue.BucketManager;
import org.eclipse.jnosql.mapping.keyvalue.AbstractKeyValueTemplate;
import org.eclipse.jnosql.mapping.keyvalue.KeyValueEntityConverter;
import org.eclipse.jnosql.mapping.keyvalue.KeyValueEventPersistManager;
import org.eclipse.jnosql.mapping.keyvalue.KeyValueWorkflow;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
@Typed(HazelcastTemplate.class)
class DefaultHazelcastTemplate extends AbstractKeyValueTemplate implements HazelcastTemplate {

    @Inject
    private Instance<HazelcastBucketManager> manager;

    @Inject
    private KeyValueWorkflow flow;

    @Inject
    private KeyValueEntityConverter converter;

    @Inject
    private KeyValueEventPersistManager persistManager;


    @Override
    public <T> Collection<T> sql(String query) {
        return manager.get().sql(query).stream().map(v -> (T) v.get()).collect(Collectors.toList());
    }

    @Override
    public <T> Collection<T> sql(String query, Map<String, Object> params) {
        return manager.get().sql(query, params).stream().map(v -> (T) v.get()).collect(Collectors.toList());
    }

    @Override
    public <K, V> Collection<V> sql(Predicate<K, V> predicate) {
        return manager.get().sql(predicate).stream().map(v -> (V) v.get()).collect(Collectors.toList());
    }

    @Override
    protected KeyValueEntityConverter getConverter() {
        return converter;
    }

    @Override
    protected BucketManager getManager() {
        return manager.get();
    }

    @Override
    protected KeyValueWorkflow getFlow() {
        return flow;
    }

    @Override
    protected KeyValueEventPersistManager getEventManager() {
        return persistManager;
    }
}
