package org.jnosql.diana.redis.key;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class DefaultSortedSet implements SortedSet {


    private String keyWithNameSpace;

    private Jedis jedis;

    DefaultSortedSet(Jedis jedis, String keyWithNameSpace) {
        this.keyWithNameSpace = keyWithNameSpace;
        this.jedis = jedis;
    }

    @Override
    public void add(String member, Number value) throws NullPointerException {
        jedis.zadd(keyWithNameSpace, value.doubleValue(), member);
    }

    @Override
    public Number increment(String member, Number value) throws NullPointerException {
        return jedis.zincrby(keyWithNameSpace, value.doubleValue(), member).longValue();
    }

    @Override
    public Number decrement(String member, Number value) throws NullPointerException {
        return increment(member, -value.longValue());
    }

    @Override
    public void remove(String member) throws NullPointerException {
        jedis.zrem(keyWithNameSpace, member);
    }

    @Override
    public int size() {
        return jedis.zcard(keyWithNameSpace).intValue();
    }

    @Override
    public void delete() {
        jedis.del(keyWithNameSpace);
    }


    @Override
    public void expire(Duration ttl) throws NullPointerException {
        jedis.expire(keyWithNameSpace, (int) ttl.getSeconds());
    }

    @Override
    public void persist() {
        jedis.persist(keyWithNameSpace);
    }

    @Override
    public List<Ranking> range(long start, long end) {
        List<Ranking> rankings = new ArrayList<>();
        Set<Tuple> scores = jedis.zrevrangeWithScores(keyWithNameSpace, start, end);
        for (Tuple tuple : scores) {
            rankings.add(new DefaultRanking(tuple.getElement(), tuple.getScore()));
        }
        return rankings;
    }

    @Override
    public List<Ranking> getRanking() {
        return range(0, size()-1);
    }
}
