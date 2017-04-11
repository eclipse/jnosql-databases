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
import redis.clients.jedis.Tuple;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The default {@link SortedSet} implementation
 */
class DefaultSortedSet implements SortedSet {


    private String key;

    private Jedis jedis;

    DefaultSortedSet(Jedis jedis, String keyspace) {
        Objects.requireNonNull(jedis, "jedis is required");
        Objects.requireNonNull(keyspace, "keyspace is required");
        this.key = keyspace;
        this.jedis = jedis;
    }

    @Override
    public void add(String member, Number value) throws NullPointerException {
        Objects.requireNonNull(member, "member is required");
        Objects.requireNonNull(value, "value is required");
        jedis.zadd(key, value.doubleValue(), member);
    }

    @Override
    public Number increment(String member, Number value) throws NullPointerException {
        Objects.requireNonNull(member, "member is required");
        Objects.requireNonNull(value, "value is required");
        return jedis.zincrby(key, value.doubleValue(), member).longValue();
    }

    @Override
    public Number decrement(String member, Number value) throws NullPointerException {
        Objects.requireNonNull(member, "member is required");
        Objects.requireNonNull(value, "value is required");
        return increment(member, -value.longValue());
    }

    @Override
    public void remove(String member) throws NullPointerException {
        jedis.zrem(key, member);
    }

    @Override
    public int size() {
        return jedis.zcard(key).intValue();
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
    public List<Ranking> range(long start, long end) {
        List<Ranking> rankings = new ArrayList<>();
        Set<Tuple> scores = jedis.zrevrangeWithScores(key, start, end);
        for (Tuple tuple : scores) {
            rankings.add(new DefaultRanking(tuple.getElement(), tuple.getScore()));
        }
        return rankings;
    }

    @Override
    public List<Ranking> getRanking() {
        return range(0, size() - 1);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultSortedSet{");
        sb.append("key='").append(key).append('\'');
        sb.append(", jedis=").append(jedis);
        sb.append('}');
        return sb.toString();
    }
}
