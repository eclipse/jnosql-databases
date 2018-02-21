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
import com.couchbase.client.java.document.json.JsonObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.jnosql.diana.couchbase.key.DefaultCouchbaseBucketManagerFactory.QUEUE;

/**
 * The couchbase implementation to {@link Queue}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This class is a wrapper to {@link com.couchbase.client.java.datastructures.collections.CouchbaseQueue}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link javax.json.bind.Jsonb#toJson(Object)}
 *
 * @param <T> the object to be stored.
 */
class CouchbaseQueue<T> extends CouchbaseCollection<T> implements Queue<T> {

    private static final int NOT_FOUND = -1;
    private final String bucketName;
    private final com.couchbase.client.java.datastructures.collections.CouchbaseQueue<JsonObject> queue;

    CouchbaseQueue(Bucket bucket, String bucketName, Class<T> clazz) {
        super(clazz);
        this.bucketName = bucketName + QUEUE;
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
        return queue.offer(JsonObjectCouchbaseUtil.toJson(JSONB, t));
    }

    @Override
    public T remove() {
        return poll();
    }

    @Override
    public T poll() {
        JsonObject json = queue.poll();
        if (json == null) {
            return null;
        }
        return getT(json.toString());
    }


    @Override
    public T peek() {
        JsonObject json = queue.peek();
        if (json == null) {
            return null;
        }
        return getT(json.toString());
    }


    @Override
    public T element() {
        JsonObject json = queue.element();
        if (json == null) {
            return null;
        }
        return getT(json.toString());
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
        boolean removeAll = true;
        for (Object object : collection) {
            if(!remove(object)) {
                removeAll = false;
            }
        }
        return removeAll;
    }

    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o, "object is required");
        return queue.removeIf(e -> e.toString().equals(JsonObjectCouchbaseUtil.toJson(JSONB, o).toString()));
    }

    @Override
    public boolean contains(Object o) {
        Objects.requireNonNull(o, "object is required");
        for (JsonObject jsonObject : queue) {
            if(jsonObject.toString().equals(JsonObjectCouchbaseUtil.toJson(JSONB, o).toString())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(queue.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(queue.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .toArray(Object[]::new);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        requireNonNull(t1s, "arrys is required");
        return StreamSupport.stream(queue.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .toArray(size -> t1s);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        collection.removeIf(e -> !this.contains(e));
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return collection.stream().allMatch(this::contains);
    }


}
