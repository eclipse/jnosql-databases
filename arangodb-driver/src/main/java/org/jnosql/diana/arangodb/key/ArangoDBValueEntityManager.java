/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.arangodb.key;


import com.arangodb.ArangoDB;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class ArangoDBValueEntityManager implements BucketManager {


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
        arangoDB.db(bucketName).collection(namespace)
                .insertDocument(new ArangoDBEntity(key.toString(), value));
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
        ArangoDBEntity entity = arangoDB.db(bucketName).collection(namespace)
                .getDocument(key.toString(), ArangoDBEntity.class);

        return ofNullable(entity).map(ArangoDBEntity::toValue);

    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        return stream(keys.spliterator(), false)
                .map(Object::toString)
                .map(k -> arangoDB.db(bucketName).collection(namespace).getDocument(k, ArangoDBEntity.class))
                .filter(Objects::nonNull)
                .map(ArangoDBEntity::toValue)
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
