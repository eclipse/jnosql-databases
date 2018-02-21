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
import com.couchbase.client.java.datastructures.collections.CouchbaseArraySet;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;
import static org.jnosql.diana.couchbase.key.DefaultCouchbaseBucketManagerFactory.SET;


/**
 * The couchbase implementation to {@link Set}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This class is a wrapper to {@link CouchbaseArraySet}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link javax.json.bind.Jsonb#toJson(Object)}
 *
 * @param <T> the object to be stored.
 */
class CouchbaseSet<T> extends CouchbaseCollection<T> implements Set<T> {

    private static final int NOT_FOUND = -1;


    private final String bucketName;
    private final CouchbaseArraySet<JsonObject> arraySet;

    CouchbaseSet(Bucket bucket, String bucketName, Class<T> clazz) {
        super(clazz);
        this.bucketName = bucketName + SET;
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
        return arraySet.add(JsonObjectCouchbaseUtil.toJson(JSONB, t));
    }

    @Override
    public boolean remove(Object o) {
        requireNonNull(o, "object is required");
        int index = indexOf(o);
        if (index >= 0) {
            arraySet.remove(index);
            return true;
        }
        return false;
    }

    @Override
    public boolean contains(Object o) {
        requireNonNull(o, "object is required");
        return indexOf(o) != NOT_FOUND;
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
        return collection.stream().map(this::contains).allMatch(TRUE::equals);
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(arraySet.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .collect(Collectors.toList()).iterator();
    }

    @Override
    public Object[] toArray() {
        return StreamSupport.stream(arraySet.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .toArray(Object[]::new);
    }

    @Override
    public <T1> T1[] toArray(T1[] t1s) {
        requireNonNull(t1s, "arrys is required");
        return StreamSupport.stream(arraySet.spliterator(), false)
                .map(s -> JSONB.fromJson(s.toString(), clazz))
                .toArray(size -> t1s);
    }


    @Override
    public boolean retainAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        collection.removeIf(e -> indexOf(e) == NOT_FOUND);
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        requireNonNull(collection, "collection is required");
        boolean removeAll = true;
        for (Object object : collection) {
            if (!this.remove(object)) {
                removeAll = false;
            }
        }
        return removeAll;
    }

    private int indexOf(Object o) {
        requireNonNull(o, "object is required");
        int index = 0;
        for (JsonObject jsonObject : arraySet) {
            if (jsonObject.toString().equals(JsonObjectCouchbaseUtil.toJson(JSONB, o).toString())) {
                return index;
            }
            index++;
        }
        return NOT_FOUND;
    }


}
