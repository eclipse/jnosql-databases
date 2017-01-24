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
package org.jnosql.diana.couchbase.key;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.datastructures.collections.CouchbaseArraySet;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * The couchbase implementation to {@link Set}
 * that avoid null items, so if any null object will launch {@link NullPointerException}
 * This implementation support these methods:
 * <p>{@link Set#isEmpty()}</p>
 * <p>{@link Set#size()}</p>
 * <p>{@link Set#add(Object)}</p>
 * <p>{@link Set#addAll(Collection)}</p>
 * <p>{@link Set#clear()}</p>
 * <p>{@link Set#remove(Object)}</p>
 * <p>{@link Set#contains(Object)}</p>
 * <p>{@link Set#containsAll(Collection)}</p>
 *
 * @param <T> the object to be stored.
 */
public class CouchbaseSet<T> implements Set<T> {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();

    private final String bucketName;
    private final Class<T> clazz;
    private final CouchbaseArraySet<String> couchbaseArraySet;

    CouchbaseSet(Bucket bucket, String bucketName, Class<T> clazz) {
        this.bucketName = bucketName + ":set";
        this.clazz = clazz;
        this.couchbaseArraySet = new CouchbaseArraySet(this.bucketName, bucket);
    }

    @Override
    public int size() {
        return couchbaseArraySet.size();
    }

    @Override
    public boolean isEmpty() {
        return couchbaseArraySet.isEmpty();
    }

    @Override
    public boolean add(T t) {
        Objects.requireNonNull(t, "object is required");
        return couchbaseArraySet.add(PROVDER.toJson(t));
    }

    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o, "object is required");
        return couchbaseArraySet.remove(PROVDER.toJson(o));
    }

    @Override
    public boolean contains(Object o) {
        Objects.requireNonNull(o, "object is required");
        return couchbaseArraySet.contains(PROVDER.toJson(o));
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        Objects.requireNonNull(collection, "collection is required");
        collection.forEach(this::add);
        return true;
    }

    @Override
    public void clear() {
        couchbaseArraySet.clear();
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        Objects.requireNonNull(collection, "collection is required");
        return collection.stream().allMatch(this::contains);
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(couchbaseArraySet.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(couchbaseArraySet.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .toArray(size -> new Object[size]);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        return StreamSupport.stream(couchbaseArraySet.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .toArray(size -> t1s);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        Objects.requireNonNull(collection, "collection is required");
        return couchbaseArraySet.retainAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        Objects.requireNonNull(collection, "collection is required");
        return couchbaseArraySet.removeAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }


}
