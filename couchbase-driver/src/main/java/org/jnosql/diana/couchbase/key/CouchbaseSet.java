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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;


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
 * @param <T> the object to be stored.
 */
public class CouchbaseSet<T> implements Set<T> {

    private final Bucket bucket;

    private final String bucketName;
    private final Class<T> clazz;

    CouchbaseSet(Bucket bucket, String bucketName, Class<T> clazz) {
        this.bucket = bucket;
        this.bucketName = bucketName;
        this.clazz = clazz;
    }

    @Override
    public int size() {
        return bucket.setSize(bucketName);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean add(T t) {
        Objects.requireNonNull(t, "object is required");
        bucket.setAdd(bucketName, t);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o, "object is required");
        return bucket.setRemove(bucketName, o) != null;
    }

    @Override
    public boolean contains(Object o) {
        Objects.requireNonNull(o, "object is required");
        return bucket.setContains(bucketName, o);
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        Objects.requireNonNull(collection, "collection is required");
        collection.forEach(this::add);
        return true;
    }

    @Override
    public void clear() {
        bucket.remove(bucketName);
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        Objects.requireNonNull(collection, "collection is required");
        return collection.stream().allMatch(this::contains);
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Couchbase does not support the iterator() method");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Couchbase does not support the toArray() method");
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        throw new UnsupportedOperationException("Couchbase does not support the toArray(T1[] t1s method");
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support the retainAll(Collection<?> collection) method");
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support the removeAll(Collection<?> collection) method");
    }


}
