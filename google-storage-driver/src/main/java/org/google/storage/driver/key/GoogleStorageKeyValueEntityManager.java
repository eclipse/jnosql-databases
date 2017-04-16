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
package org.google.storage.driver.key;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;
import org.jnosql.diana.driver.value.JSONValueProvider;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class GoogleStorageKeyValueEntityManager implements BucketManager {

    private final Storage storage;
    private final String bucket;
    private final JSONValueProvider provider;

    public GoogleStorageKeyValueEntityManager(Storage storage, String bucket, JSONValueProvider provider) {
        this.storage = storage;
        this.bucket = bucket;
        this.provider = provider;
    }

    @Override
    public <K, V> void put(K key, V value) throws NullPointerException {


        Objects.requireNonNull(value, "Value is required");
        Objects.requireNonNull(key, "key is required");

        try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8))) {
            BlobId blobId = BlobId.of(bucket, key.toString());
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build();
            storage.create(blobInfo, inputStream);
        } catch (IOException e) {
            throw new GoogleStorageException("An error when put the key " + key, e);
        }
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        put(entity.getKey(), entity.getValue());

    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl)
            throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("The google storage does not support getList method");
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> entities) throws NullPointerException {
        StreamSupport.stream(entities.spliterator(), false).forEach(this::put);

    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> entities, Duration ttl)
            throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("The google storage does not support getList method");
    }

    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {

        Objects.requireNonNull(key, "key is required");

        BlobId blobId = BlobId.of(bucket, key.toString());
        byte[] readAllBytes = storage.readAllBytes(blobId);

        if (readAllBytes.length > 0) {
            Value value = provider.of(readAllBytes);
            return Optional.of(value);
        } else return Optional.empty();
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> BlobId.of(bucket, k.toString()))
                .map(b -> storage.readAllBytes(b))
                .map(r -> provider.of(r))
                .collect(Collectors.toList());
    }

    @Override
    public <K> void remove(K key) throws NullPointerException {

        Objects.requireNonNull(key, "key is required");

        BlobId blobId = BlobId.of(bucket, key.toString());
        storage.delete(blobId);
    }

    @Override
    public <K> void remove(Iterable<K> keys) throws NullPointerException {
        StreamSupport.stream(keys.spliterator(), false).forEach(this::remove);
    }

    @Override
    public void close() {
    }

}
