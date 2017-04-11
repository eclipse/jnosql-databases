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

/**
 * The redis counter structure
 */
public interface Counter {

    /**
     * @return The counter value
     */
    Number get();

    /**
     * Increments by one the counter
     *
     * @return the increment by one result
     */
    Number increment();

    /**
     * Increments the counter
     *
     * @param value the value to be increased
     * @return the increment result
     * @throws NullPointerException when value is null
     */
    Number increment(Number value) throws NullPointerException;

    /**
     * Decrements by one the counter
     *
     * @return the increment by one result
     */
    Number decrement();

    /**
     * Decrements
     *
     * @param value the value to be decreased
     * @return the decrement result
     */
    Number decrement(Number value);

    /**
     * Delete this Counter
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
}
