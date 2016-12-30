/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.riak.key;

import java.time.Duration;
import java.util.Objects;

import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Builder;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

public final class RiakUtils {

    private RiakUtils() {
    }

    public static <K, V> StoreValue createStoreValue(K key, V value, Namespace namespace, Duration ttl) {

        Objects.requireNonNull(value, "Value is required");
        Objects.requireNonNull(key, "key is required");

        Location location = createLocation(namespace, key);
        Builder builder = new StoreValue.Builder(value).withLocation(location);

        if (!ttl.isZero()) {
            builder = builder.withTimeout(Math.toIntExact(ttl.getSeconds()));
        }


        return builder.build();
    }

    public static <K> Location createLocation(Namespace namespace, K key) {

        Objects.requireNonNull(namespace, "Namespace is required");
        Objects.requireNonNull(key, "key is required");

        return new Location(namespace, key.toString());
    }

    public static <K> FetchValue createFetchValue(Namespace namespace, K key) {

        Location location = createLocation(namespace, key);
        return new FetchValue.Builder(location).build();
    }

    public static <K> DeleteValue createDeleteValue(Namespace namespace, K key) {

        Location location = createLocation(namespace, key);
        return new DeleteValue.Builder(location).build();
    }


}
