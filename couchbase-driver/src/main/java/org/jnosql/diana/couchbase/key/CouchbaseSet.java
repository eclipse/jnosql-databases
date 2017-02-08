/*
 * Copyright 2017 Eclipse Foundation
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;


/**
 * The couchbase implementation to {@link Set}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This class is a wrapper to {@link CouchbaseArraySet}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link JSONValueProvider#toJson(Object)}
 * @param <T> the object to be stored.
 */
public class CouchbaseSet<T> implements Set<T> {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();

    private final String bucketName;
    private final Class<T> clazz;
    private final CouchbaseArraySet<String> arraySet;

    CouchbaseSet(Bucket bucket, String bucketName, Class<T> clazz) {
        this.bucketName = bucketName + ":set";
        this.clazz = clazz;
        this.arraySet = new CouchbaseArraySet(this.bucketName, bucket);
    }

    @Override
    public int size() {
        return arraySet.size();
    }

    @Override
    public boolean isEmpty() {
        return arraySet.isEmpty();
    }

    @Override
    public boolean add(T t) {
        requireNonNull(t, "object is required");
        return arraySet.add(PROVDER.toJson(t));
    }

    @Override
    public boolean remove(Object o) {
        requireNonNull(o, "object is required");
        return arraySet.remove(PROVDER.toJson(o));
    }

    @Override
    public boolean contains(Object o) {
        requireNonNull(o, "object is required");
        return arraySet.contains(PROVDER.toJson(o));
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        requireNonNull(collection, "collection is required");
        collection.forEach(this::add);
        return true;
    }

    @Override
    public void clear() {
        arraySet.clear();
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return collection.stream().allMatch(this::contains);
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(arraySet.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(arraySet.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .toArray(size -> new Object[size]);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        requireNonNull(t1s, "arrys is required");
        return StreamSupport.stream(arraySet.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .toArray(size -> t1s);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return arraySet.retainAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return arraySet.removeAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }


}
