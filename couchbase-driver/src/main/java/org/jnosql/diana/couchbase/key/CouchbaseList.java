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
import com.couchbase.client.java.datastructures.collections.CouchbaseArrayList;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.couchbase.key.DefaultCouchbaseBucketManagerFactory.LIST;

/**
 * The couchbase implementation to {@link List}
 * that avoid null items, so if any null object will launch {@link NullPointerException}
 * This class is a wrapper to {@link CouchbaseArrayList}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link javax.json.bind.Jsonb#toJson(Object)}
 *
 * @param <T> the object to be stored.
 */
class CouchbaseList<T> extends CouchbaseCollection<T> implements List<T> {

    private static final int NOT_FOUND = -1;
    private final String bucketName;
    private final CouchbaseArrayList<JsonObject> arrayList;

    CouchbaseList(Bucket bucket, String bucketName, Class<T> clazz) {
        super(clazz);
        this.bucketName = bucketName + LIST;
        this.arrayList = new CouchbaseArrayList(this.bucketName, bucket);
    }

    @Override
    public int size() {
        return arrayList.size();
    }

    @Override
    public boolean isEmpty() {
        return arrayList.isEmpty();
    }


    @Override
    public boolean add(T t) {
        requireNonNull(t, "object is required");
        return arrayList.add(JsonObjectCouchbaseUtil.toJson(JSONB, t));
    }

    @Override
    public boolean addAll(Collection<? extends T> collection) {
        requireNonNull(collection, "collection is required");
        collection.forEach(this::add);
        return true;
    }


    @Override
    public void clear() {
        arrayList.clear();

    }

    @Override
    public T get(int i) {
        JsonObject jsonObject = arrayList.get(i);
        return JSONB.fromJson(jsonObject.toString(), clazz);
    }

    @Override
    public T set(int i, T t) {
        requireNonNull(t, "object is required");
        JsonObject json = arrayList.set(i, JsonObjectCouchbaseUtil.toJson(JSONB, t));
        if (Objects.nonNull(json)) {
            return JSONB.fromJson(json.toString(), clazz);
        }
        return null;
    }


    @Override
    public T remove(int i) {
        JsonObject json = arrayList.remove(i);
        if (Objects.nonNull(json)) {
            return JSONB.fromJson(json.toString(), clazz);
        }
        return null;
    }


    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(arrayList.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .collect(toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(arrayList.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .toArray(Object[]::new);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        requireNonNull(t1s, "arrys is required");
        return StreamSupport.stream(arrayList.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .toArray(size -> t1s);
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return arrayList.retainAll(collection.stream().map(JSONB::toJson).collect(toList()));
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return arrayList.removeAll(collection.stream().map(JSONB::toJson).collect(toList()));
    }

    @Override
    public void add(int i, T t) {
        requireNonNull(t, "object is required");
        arrayList.add(i, JsonObjectCouchbaseUtil.toJson(JSONB, t));
    }

    @Override
    public int indexOf(Object o) {
        requireNonNull(o, "object is required");
        int index = 0;
        for (JsonObject jsonObject : arrayList) {
            if (jsonObject.toString().equals(JsonObjectCouchbaseUtil.toJson(JSONB, o).toString())) {
                return index;
            }
            index++;
        }
        return NOT_FOUND;
    }

    @Override
    public int lastIndexOf(Object o) {
        requireNonNull(o, "object is required");
        for (int index = arrayList.size() - 1; index >= 0; index--) {
            JsonObject jsonObject = arrayList.get(index);
            if (jsonObject.toString().equals(JsonObjectCouchbaseUtil.toJson(JSONB, o).toString())) {
                return index;
            }
        }
        return NOT_FOUND;
    }

    @Override
    public ListIterator<T> listIterator() {
        return StreamSupport.stream(spliteratorUnknownSize(arrayList.listIterator(), Spliterator.ORDERED),
                false).map(s -> JSONB.fromJson(s.toString(), clazz))
                .collect(toList())
                .listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int i) {
        return StreamSupport.stream(spliteratorUnknownSize(arrayList.listIterator(i), Spliterator.ORDERED),
                false).map(s -> JSONB.fromJson(s.toString(), clazz))
                .collect(toList())
                .listIterator();
    }

    @Override
    public List<T> subList(int i, int i1) {
        return arrayList.subList(i, i1).stream().
                map(s -> JSONB.fromJson(s.toString(), clazz))
                .collect(toList());
    }

    @Override
    public boolean remove(Object o) {
        requireNonNull(o, "object is required");
        int index = indexOf(o);
        if (index >= 0) {
            arrayList.remove(index);
            return true;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        return arrayList.containsAll(collection.stream()
                .map(s -> JsonObjectCouchbaseUtil.toJson(JSONB, s))
                .collect(toList()));
    }

    @Override
    public boolean contains(Object o) {
        requireNonNull(o, "object is required");
        return arrayList.contains(JsonObjectCouchbaseUtil.toJson(JSONB, o));
    }

    @Override
    public boolean addAll(int i, Collection<? extends T> collection) {
        requireNonNull(collection, "collection is required");
        List<JsonObject> objects = collection.stream().map(s -> JsonObjectCouchbaseUtil.toJson(JSONB, s)).collect(toList());
        return arrayList.addAll(i, objects);
    }


}
