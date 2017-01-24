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
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation to {@link Queue}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This implementation support these methods:
 * <p>{@link Queue#size()}</p>
 * <p>{@link Queue#isEmpty()}</p>
 * <p>{@link Queue#add(Object)}</p>
 * <p>{@link Queue#addAll(Collection)}</p>
 * <p>{@link Queue#clear()}</p>
 * <p>{@link Queue#offer(Object)}</p>
 * <p>{@link Queue#remove()}</p>
 * <p>{@link Queue#poll()}</p>
 *
 * @param <T> the object to be stored.
 */
public class CouchbaseQueue<T> implements Queue<T> {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();


    private final String bucketName;
    private final Class<T> clazz;
    private final com.couchbase.client.java.datastructures.collections.CouchbaseQueue<String> queue;

    CouchbaseQueue(Bucket bucket, String bucketName, Class<T> clazz) {
        this.bucketName = bucketName + ":queue";
        this.clazz = clazz;
        queue = new com.couchbase.client.java.datastructures.collections.CouchbaseQueue<>(this.bucketName, bucket);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
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
        queue.clear();
    }

    @Override
    public boolean offer(T t) {
        requireNonNull(t, "object is required");
        return queue.offer(PROVDER.toJson(t));
    }

    @Override
    public T remove() {
        return poll();
    }

    @Override
    public T poll() {
        String json = queue.poll();
        return getT(json);
    }


    @Override
    public T peek() {
        String json = queue.peek();
        return getT(json);
    }


    @Override
    public T element() {
        String json = queue.element();
        return getT(json);
    }

    private T getT(String json) {
        if (Objects.nonNull(json)) {
            return PROVDER.of(json).get(clazz);
        }
        return null;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return queue.removeAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }

    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o, "object is required");
        return queue.remove(PROVDER.toJson(o));
    }

    @Override
    public boolean contains(Object o) {
        Objects.requireNonNull(o, "object is required");
        return queue.contains(PROVDER.toJson(o));
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(queue.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(queue.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .toArray(size -> new Object[size]);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        requireNonNull(t1s, "arrys is required");
        return StreamSupport.stream(queue.spliterator(), false)
                .map(s -> PROVDER.of(s).get(clazz))
                .toArray(size -> t1s);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return queue.retainAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return queue.containsAll(collection.stream().map(PROVDER::toJson).collect(Collectors.toList()));
    }

}
