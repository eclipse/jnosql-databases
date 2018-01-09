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

package org.jnosql.diana.redis.key;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

abstract class RedisStringCollection implements Collection<String> {

    protected String keyWithNameSpace;

    protected Jedis jedis;



    RedisStringCollection(Jedis jedis, String keyWithNameSpace) {
        this.keyWithNameSpace = keyWithNameSpace;
        this.jedis = jedis;
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        Objects.requireNonNull(c);
        for (String bean : c) {
            if (bean != null) {
                add(bean);
            }
        }
        return true;
    }

    @Override
    public int size() {
        return jedis.llen(keyWithNameSpace).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    @Override
    public Iterator<String> iterator() {
        return toArrayList().iterator();
    }

    @Override
    public Object[] toArray() {
        return toArrayList().toArray();
    }

    @Override
    public <E> E[] toArray(E[] a) {
        return toArrayList().toArray(a);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Use add all instead");
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsAll(Collection<?> elements) {
        Objects.requireNonNull(elements, "elements is required");
        boolean containsAll = true;
        for (String element : (Collection<String>) elements) {
            if (!contains(element)) {
                containsAll = false;
            }
        }
        return containsAll;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeAll(Collection<?> elements) {
        Objects.requireNonNull(elements, "elements is required");
        boolean result = false;
        for (String element : (Collection<String>) elements) {
            if (remove(element)) {
                result = true;
            }
        }
        return result;
    }

    @Override
    public boolean remove(Object o) {
        if (!String.class.isInstance(o)) {
            throw new ClassCastException("The object required is " + String.class.getName());
        }
        int index = indexOf(o);

        if (index == -1) {
            return false;
        } else {
            remove(index);
        }

        return true;
    }

    protected String remove(int index) {
        String value = jedis.lindex(keyWithNameSpace, (long) index);
        if (value != null && !value.isEmpty()) {
            jedis.lrem(keyWithNameSpace, 1, value);
            return value;
        }
        return null;
    }

    protected int indexOf(Object o) {
        if (!String.class.isInstance(o)) {
            return -1;
        }

        String value = o.toString();
        for (int index = 0; index < size(); index++) {
            String findedValue = jedis.lindex(keyWithNameSpace, (long) index);
            if (value.equals(findedValue)) {
                return index;
            }
        }
        return -1;
    }

    protected List<String> toArrayList() {
        List<String> list = new ArrayList<>();
        for (int index = 0; index < size(); index++) {
            String element = get(index);
            if (element != null) {
                list.add(element);
            }
        }
        return list;
    }

    protected String get(int index) {
        String value = jedis.lindex(keyWithNameSpace, index);
        if (value == null || value.isEmpty()) {
            return null;
        }
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keyWithNameSpace);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (RedisStringCollection.class.isInstance(obj)) {
            RedisStringCollection otherRedis = RedisStringCollection.class.cast(obj);
            return Objects.equals(otherRedis.keyWithNameSpace, keyWithNameSpace);
        }
        return false;
    }

}
