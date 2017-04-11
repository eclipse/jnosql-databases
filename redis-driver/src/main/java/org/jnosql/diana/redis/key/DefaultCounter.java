/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.redis.key;

import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;


/**
 * The default {@link Counter}
 */
class DefaultCounter implements Counter {

    private static final Predicate<String> IS_EMPTY = String::isEmpty;
    private static final Predicate<String> IS_NOT_EMPTY = IS_EMPTY.negate();

    private final String key;

    private Jedis jedis;

    DefaultCounter(String key, Jedis jedis) {
        this.key = key;
        this.jedis = jedis;
    }


    @Override
    public Number get() {
        return Optional.ofNullable(jedis.get(key))
                .filter(IS_NOT_EMPTY)
                .map(Double::valueOf)
                .orElse(0D);
    }

    @Override
    public Number increment() {
        return increment(1);
    }

    @Override
    public Number increment(Number value) throws NullPointerException {
        Objects.requireNonNull(value, "value is required");
        return jedis.incrByFloat(key, value.doubleValue());
    }

    @Override
    public Number decrement() {
        return increment(-1);
    }

    @Override
    public Number decrement(Number value) {
        Objects.requireNonNull(value, "value is required");
        return jedis.incrByFloat(key, -value.doubleValue());
    }

    @Override
    public void delete() {
        jedis.del(key);
    }

    @Override
    public void expire(Duration ttl) throws NullPointerException {
        Objects.requireNonNull(ttl, "ttl is required");
        jedis.expire(key, (int) ttl.getSeconds());
    }

    @Override
    public void persist() {
        jedis.persist(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultCounter)) {
            return false;
        }
        DefaultCounter that = (DefaultCounter) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Counter{");
        sb.append("key='").append(key).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
