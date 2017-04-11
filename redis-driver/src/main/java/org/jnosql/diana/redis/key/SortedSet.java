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


import java.time.Duration;
import java.util.List;

/**
 * Sorted sets are a data type which is similar to a mix between a Set and a Hash.
 * Like sets, sorted sets are composed of unique, non-repeating string elements,
 * so in some sense a sorted set is a set as well.
 */
public interface SortedSet {

    /**
     * Adds all the specified members with the specified scores to the sorted set stored at key.
     * Creates a score.
     *
     * @param member the name
     * @param value  the value
     * @throws NullPointerException when either member or value are null
     */
    void add(String member, Number value) throws NullPointerException;


    /**
     * Adds all the specified members with the specified scores to the sorted set stored at key.
     * Creates a score.
     *
     * @param ranking the element
     * @throws NullPointerException when ranking is null
     */
    void add(Ranking ranking) throws NullPointerException;

    /**
     * Increments the score of member in the sorted set stored at member by increment.
     *
     * @param member the member
     * @param value  the vaue
     * @return the new score member
     * @throws NullPointerException when either member or value are null
     */
    Number increment(String member, Number value) throws NullPointerException;

    /**
     * Increments the score of member in the sorted set stored at member by increment.
     *
     * @param member the member
     * @param value  the vaue
     * @return the new score member
     * @throws NullPointerException when either member or value are null
     */
    Number decrement(String member, Number value) throws NullPointerException;

    /**
     * Removes a member
     *
     * @param member the member
     * @throws NullPointerException when member is null
     */
    void remove(String member) throws NullPointerException;

    /**
     * @return the number of members on this
     */
    int size();

    /**
     * @return Returns true if this SortedSet contains no elements.
     */
    boolean isEmpty();

    /**
     * Delete this SortedSet
     */
    void delete();

    /**
     * Defines a ttl to SortedSet
     *
     * @param ttl the ttl
     * @throws NullPointerException when either key and ttl are null
     */
    void expire(Duration ttl) throws NullPointerException;

    /**
     * Removes the ttl
     */
    void persist();

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * The elements are considered to be ordered from the lowest to the highest score.
     * Lexicographical order is used for elements with equal score.
     * Both start and stop are zero-based indexes
     *
     * @param start the index to start
     * @param end   the index to end
     * @return the Ranking
     */
    List<Ranking> range(long start, long end);

    /**
     * Returns the specified range of elements in the sorted set
     * stored at key. The elements are considered to be ordered from the
     * highest to the lowest score. Descending lexicographical order is used for elements with equal score.
     * Both start and stop are zero-based indexes
     *
     * @param start the index to start
     * @param end   the index to end
     * @return the Ranking
     */
    List<Ranking> revRange(long start, long end);

    /**
     * Returns all elements using {@link SortedSet#range(long, long)}
     *
     * @return the rankings
     * @see SortedSet#range(long, long)
     */
    List<Ranking> getRanking();

    /**
     * Returns all elements using {@link SortedSet#revRange(long, long)}
     *
     * @return the rankings
     * @see SortedSet#revRange(long, long)
     */
    List<Ranking> getRevRanking();
}