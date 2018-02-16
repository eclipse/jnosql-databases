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
package org.jnosql.diana.couchbase.key;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;
import org.jnosql.diana.driver.ValueJSON;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.driver.ValueUtil.convert;

/**
 * The couchbase implementation to {@link BucketManager}
 */
public class CouchbaseBucketManager implements BucketManager {

    private static final Jsonb JSONB = JsonbBuilder.create();

    private final Bucket bucket;

    private final String bucketName;

    CouchbaseBucketManager(Bucket bucket, String bucketName) {
        this.bucket = bucket;
        this.bucketName = bucketName;
    }

    @Override
    public <K, V> void put(K key, V value) throws NullPointerException {
        requireNonNull(key, "key is required");
        requireNonNull(value, "value is required");
        bucket.upsert(JsonDocument.create(key.toString(), JsonObjectCouchbaseUtil.toJson(JSONB, value)));
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        put(entity.getKey(), convert(entity.getValue()));
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");


        JsonObject jsonObject = JsonObjectCouchbaseUtil.toJson(JSONB, entity.get());

        JsonDocument jsonDocument = JsonDocument.create(entity.getKey().toString(), (int) ttl.getSeconds(), jsonObject);
        bucket.upsert(jsonDocument, ttl.toMillis(), MILLISECONDS);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> keyValueEntities) throws NullPointerException {
        requireNonNull(keyValueEntities, "keyValueEntities is required");
        keyValueEntities.forEach(this::put);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> keyValueEntities, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        requireNonNull(keyValueEntities, "keyValueEntities is required");
        requireNonNull(ttl, "ttl is required");
        keyValueEntities.forEach(k -> this.put(k, ttl));
    }

    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {
        requireNonNull(key, "key is required");
        JsonDocument jsonDocument = bucket.get(key.toString());
        if (Objects.isNull(jsonDocument)) {
            return Optional.empty();
        }
        Object value = jsonDocument.content();
        return Optional.of(ValueJSON.of(value.toString()));
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        requireNonNull(keys, "keys is required");
        return stream(keys.spliterator(), false)
                .map(this::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
    }

    @Override
    public <K> void remove(K key) throws NullPointerException {
        requireNonNull(key, "key is required");
        bucket.remove(key.toString());
    }

    @Override
    public <K> void remove(Iterable<K> keys) throws NullPointerException {
        requireNonNull(keys, "keys is required");
        keys.forEach(this::remove);
    }

    @Override
    public void close() {
        bucket.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CouchbaseBucketManager{");
        sb.append("bucket=").append(bucket);
        sb.append(", bucketName='").append(bucketName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
