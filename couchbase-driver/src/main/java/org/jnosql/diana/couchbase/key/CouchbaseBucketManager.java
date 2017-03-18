/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.couchbase.key;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.driver.value.ValueUtil.convert;

/**
 * The couchbase implementation to {@link BucketManager}
 */
public class CouchbaseBucketManager implements BucketManager {

    private static final String VALUE_FIELD = "value";

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();

    private final Bucket bucket;

    private final String bucketName;

    CouchbaseBucketManager(Bucket bucket, String bucketName) {
        this.bucket = bucket;
        this.bucketName = bucketName;
    }

    @Override
    public <K, V> void put(K key, V value) throws NullPointerException {
        Objects.requireNonNull(key, "key is required");
        Objects.requireNonNull(value, "value is required");
        JsonObject jsonObject = JsonObject.create()
                .put("value", PROVDER.toJson(value));

        bucket.upsert(JsonDocument.create(key.toString(), jsonObject));
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        Objects.requireNonNull(entity, "entity is required");
        put(entity.getKey(), convert(entity.getValue()));
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(entity, "entity is required");
        Objects.requireNonNull(ttl, "ttl is required");
        JsonObject jsonObject = JsonObject.create()
                .put(VALUE_FIELD, entity.getValue().get());

        bucket.upsert(JsonDocument.create(entity.getKey().toString(), jsonObject), ttl.toMillis(), MILLISECONDS);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> keyValueEntities) throws NullPointerException {
        Objects.requireNonNull(keyValueEntities, "keyValueEntities is required");
        keyValueEntities.forEach(this::put);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> keyValueEntities, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(keyValueEntities, "keyValueEntities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        keyValueEntities.forEach(k -> this.put(k, ttl));
    }

    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {
        Objects.requireNonNull(key, "key is required");
        JsonDocument jsonDocument = bucket.get(key.toString());
        if (Objects.isNull(jsonDocument)) {
            return Optional.empty();
        }
        Object value = jsonDocument.content().get(VALUE_FIELD);
        return Optional.of(PROVDER.of(value.toString()));
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        Objects.requireNonNull(keys, "keys is required");
        return stream(keys.spliterator(), false)
                .map(this::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
    }

    @Override
    public <K> void remove(K key) throws NullPointerException {
        Objects.requireNonNull(key, "key is required");
        bucket.remove(key.toString());
    }

    @Override
    public <K> void remove(Iterable<K> keys) throws NullPointerException {
        Objects.requireNonNull(keys, "keys is required");
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
