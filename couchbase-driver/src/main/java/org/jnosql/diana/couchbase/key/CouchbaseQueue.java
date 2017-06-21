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

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation to {@link Queue}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This class is a wrapper to {@link com.couchbase.client.java.datastructures.collections.CouchbaseQueue}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link Jsonb#toJson(Object)}
 *
 * @param <T> the object to be stored.
 */
public class CouchbaseQueue<T> implements Queue<T> {

    private static final Jsonb JSONB = JsonbBuilder.create();


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
        return queue.offer(JSONB.toJson(t));
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
            return JSONB.fromJson(json, clazz);
        }
        return null;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return queue.removeAll(collection.stream().map(JSONB::toJson).collect(Collectors.toList()));
    }

    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o, "object is required");
        return queue.remove(JSONB.toJson(o));
    }

    @Override
    public boolean contains(Object o) {
        Objects.requireNonNull(o, "object is required");
        return queue.contains(JSONB.toJson(o));
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(queue.spliterator(), false)
                .map(fromJSON())
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(queue.spliterator(), false)
                .map(fromJSON())
                .toArray(size -> new Object[size]);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        requireNonNull(t1s, "arrys is required");
        return StreamSupport.stream(queue.spliterator(), false)
                .map(fromJSON())
                .toArray(size -> t1s);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return queue.retainAll(collection.stream().map(JSONB::toJson).collect(Collectors.toList()));
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return queue.containsAll(collection.stream().map(JSONB::toJson).collect(Collectors.toList()));
    }

    private Function<String, T> fromJSON() {
        return s -> JSONB.fromJson(s, clazz);
    }


}
