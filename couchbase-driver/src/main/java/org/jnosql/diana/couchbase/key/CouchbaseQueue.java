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
import java.util.Queue;

import static java.util.Objects.requireNonNull;

public class CouchbaseQueue<T> implements Queue<T> {

    private final Bucket bucket;

    private final String bucketName;
    private final Class<T> clazz;

    CouchbaseQueue(Bucket bucket, String bucketName, Class<T> clazz) {
        this.bucket = bucket;
        this.bucketName = bucketName;
        this.clazz = clazz;
    }

    @Override
    public int size() {
        return bucket.queueSize(bucketName);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }


    @Override
    public boolean add(T t) {
        return offer(t);
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        requireNonNull(collection, "collection is required");
        collection.forEach(this::add);
        return true;
    }

    @Override
    public void clear() {
        bucket.remove(bucketName);
    }

    @Override
    public boolean offer(T t) {
        requireNonNull(t, "object is required");
        return bucket.queuePush(bucketName, t);
    }

    @Override
    public T remove() {
        return poll();
    }

    @Override
    public T poll() {
        return bucket.queuePop(bucketName, clazz);
    }


    @Override
    public T peek() {
        throw new UnsupportedOperationException("Couchbase does not support peek() method");
    }


    @Override
    public T element() {
        throw new UnsupportedOperationException("Couchbase does not support element() method");
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support removeAll(Collection<?> collection) method ");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Couchbase does not support remove(Object o) method ");
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("Couchbase does not support contains(Object o) method");
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Couchbase does not support iterator() method");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Couchbase does not support toArray() method");
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        throw new UnsupportedOperationException("Couchbase does not support toArray(T1[] t1s) method");
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support retainAll(Collection<?> collection) method");
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support containsAll(Collection<?> collection) method");
    }


}
