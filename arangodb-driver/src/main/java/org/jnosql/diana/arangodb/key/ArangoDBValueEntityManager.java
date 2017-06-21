/*
 *  Copyright (c) 2017 Otávio Santana and others
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
package org.jnosql.diana.arangodb.key;


import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;
import org.jnosql.diana.driver.ValueJSON;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * The ArangoDB implementation to {@link BucketManager} it does not support TTL methods:
 * <p>{@link BucketManager#put(Iterable, Duration)}</p>
 * <p>{@link BucketManager#put(Iterable, Duration)}</p>
 */
public class ArangoDBValueEntityManager implements BucketManager {


    private static final String VALUE = "_value";
    private static final Function<BaseDocument, String> TO_JSON = e -> e.getAttribute(VALUE).toString();
    private static final Jsonb JSONB = JsonbBuilder.create();

    private final ArangoDB arangoDB;

    private final String bucketName;
    private final String namespace;


    ArangoDBValueEntityManager(ArangoDB arangoDB, String bucketName, String namespace) {
        this.arangoDB = arangoDB;
        this.bucketName = bucketName;
        this.namespace = namespace;
    }

    @Override
    public <K, V> void put(K key, V value) throws NullPointerException {
        Objects.requireNonNull(key, "Key is required");
        Objects.requireNonNull(value, "value is required");
        BaseDocument baseDocument = new BaseDocument();
        baseDocument.setKey(key.toString());
        baseDocument.addAttribute(VALUE, JSONB.toJson(value));
        if (arangoDB.db(bucketName).collection(namespace).documentExists(key.toString())) {
            arangoDB.db(bucketName).collection(namespace).deleteDocument(key.toString());
        }
        arangoDB.db(bucketName).collection(namespace)
                .insertDocument(baseDocument);
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        put(entity.getKey(), entity.getValue().get());
    }


    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> keyValueEntities) throws NullPointerException {
        keyValueEntities.forEach(this::put);
    }


    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {
        Objects.requireNonNull(key, "Key is required");
        BaseDocument entity = arangoDB.db(bucketName).collection(namespace)
                .getDocument(key.toString(), BaseDocument.class);

        return ofNullable(entity)
                .map(TO_JSON)
                .map(j -> ValueJSON.of(j));

    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        return stream(keys.spliterator(), false)
                .map(Object::toString)
                .map(k -> arangoDB.db(bucketName).collection(namespace)
                        .getDocument(k, BaseDocument.class))
                .filter(Objects::nonNull)
                .map(TO_JSON)
                .map(j -> ValueJSON.of(j))
                .collect(toList());
    }

    @Override
    public <K> void remove(K key) throws NullPointerException {
        arangoDB.db(bucketName).collection(namespace).deleteDocument(key.toString());
    }

    @Override
    public <K> void remove(Iterable<K> keys) throws NullPointerException {
        Objects.requireNonNull(keys, "Keys is required");

        arangoDB.db(bucketName).collection(namespace)
                .deleteDocuments(stream(keys.spliterator(), false)
                        .map(Object::toString).collect(toList()));
    }

    @Override
    public void close() {

    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> keyValueEntities, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("ArangoDB does not support TTL");
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("ArangoDB does not support TTL");
    }

}
