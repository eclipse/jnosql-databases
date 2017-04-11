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


/**
 * Sorted sets are a data type which is similar to a mix between a Set and a Hash.
 * Like sets, sorted sets are composed of unique, non-repeating string elements,
 * so in some sense a sorted set is a set as well.
 *
 * @param <T> the number
 */
public interface SortedSetFactory<T extends Number> {

    /**
     * Creates a {@link SortedSet} from key
     *
     * @param key the key
     * @return the SortedSet from key
     * @throws NullPointerException when key is null
     */
    SortedSet<T> create(String key) throws NullPointerException;

}
