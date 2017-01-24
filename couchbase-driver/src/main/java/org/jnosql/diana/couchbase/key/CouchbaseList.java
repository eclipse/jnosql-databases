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
import java.util.ListIterator;
import java.util.Objects;

class CouchbaseList<T> implements List<T> {

    private final Bucket bucket;

    private final String bucketName;
    private final Class<T> clazz;

    CouchbaseList(Bucket bucket, String bucketName, Class<T> clazz) {
        this.bucket = bucket;
        this.bucketName = bucketName;
        this.clazz = clazz;
    }

    @Override
    public int size() {
        return bucket.listSize(bucketName);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean add(T t) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        Objects.requireNonNull(collection, "collection is required");
        collection.forEach(t -> bucket.listAppend(bucketName, t));
        return true;
    }


    @Override
    public void clear() {
        bucket.remove(bucketName);

    }

    @Override
    public T get(int i) {
        return bucket.listGet(bucketName, i, clazz);
    }

    @Override
    public T set(int i, T t) {
        Objects.requireNonNull(t, "object is required");
        bucket.listSet(bucketName, i, t);
        return t;
    }


    @Override
    public T remove(int i) {
        bucket.listRemove(bucketName, i);
        return null;
    }


    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("Couchbase does not support the contains method");
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Couchbase does not support the iterator method");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Couchbase does not support the iterator method");
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        throw new UnsupportedOperationException("Couchbase does not support the iterator method");
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support the  remove(Object) method");
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support the  remove(Object) method");
    }

    @Override
    public void add(int i, T t) {
        throw new UnsupportedOperationException("Couchbase does not support the  add(int i, T t) method");
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException("Couchbase does not support the  indexOf(Object o) method");
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("Couchbase does not support the  lastIndexOf(Object o) method");
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException("Couchbase does not support the  listIterator() method");
    }

    @Override
    public ListIterator<T> listIterator(int i) {
        throw new UnsupportedOperationException("Couchbase does not support the  listIterator(int it) method");
    }

    @Override
    public List<T> subList(int i, int i1) {
        throw new UnsupportedOperationException("Couchbase does not support the  subList(int i, int i1) method");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Couchbase does not support the  remove(Object) method");
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Couchbase does not support the contains method");
    }

    @Override
    public boolean addAll(int i, Collection<? extends T> collection) {
        throw new UnsupportedOperationException("Couchbase does not support the addAll(int i, Collection<? extends T> collection) method");
    }
}
