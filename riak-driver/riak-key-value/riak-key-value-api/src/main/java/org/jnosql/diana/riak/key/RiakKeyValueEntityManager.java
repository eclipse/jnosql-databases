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


import static org.jnosql.diana.riak.key.RiakUtils.createDeleteValue;
import static org.jnosql.diana.riak.key.RiakUtils.createFetchValue;
import static org.jnosql.diana.riak.key.RiakUtils.createStoreValue;
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.FetchValue.Response;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Namespace;
import com.google.gson.Gson;

public class RiakKeyValueEntityManager implements BucketManager {


	private final RiakClient client;
	
	private final Gson gson;
	
	private final Namespace nameSpace;
	
	public RiakKeyValueEntityManager(RiakClient client, Gson gson, Namespace nameSpace) {
		this.client = client;
		this.gson = gson;
		this.nameSpace = nameSpace;
	}

	@Override
	public <K, V> void put(K key, V value) throws NullPointerException {
		put(KeyValueEntity.of(key,value));
	}

	@Override
	public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
		put(entity,Duration.ZERO);
	}

	@Override
	public <K> void put(KeyValueEntity<K> entity, Duration ttl)
			throws NullPointerException, UnsupportedOperationException {

		K key = entity.getKey();
		Value value = entity.getValue();

		StoreValue storeValue = createStoreValue(key,value,nameSpace,ttl);
		
		try {
			client.execute(storeValue);
		} catch (ExecutionException | InterruptedException e) {
			throw new DianaRiakException(e.getMessage(),e);
		}
	}

	@Override
	public <K> void put(Iterable<KeyValueEntity<K>> entities) throws NullPointerException {
		StreamSupport.stream(entities.spliterator(),false).forEach(this::put);
	}

	@Override
	public <K> void put(Iterable<KeyValueEntity<K>> entities, Duration ttl)
			throws NullPointerException, UnsupportedOperationException {
		
		StreamSupport.stream(entities.spliterator(),false).forEach(e -> put(e,ttl));
	}

	@Override
	public <K> Optional<Value> get(K key) throws NullPointerException {
		
		if(StringUtils.isBlank(key.toString())){
			throw new DianaRiakException("The Key is irregular",new IllegalStateException());
		}

		FetchValue fetchValue = createFetchValue(nameSpace, key);
		try {
			
			FetchValue.Response response = client.execute(fetchValue);
			
			String valueFetch = response.getValue(String.class);
			if(StringUtils.isNoneBlank(valueFetch))
				return Optional.of(RiakValue.of(gson,valueFetch));
			
		} catch (ExecutionException | InterruptedException e) {
			throw new DianaRiakException(e.getMessage(),e);
		}
		return Optional.empty();
	}

	@Override
	public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
		
		return	StreamSupport.stream(keys.spliterator(),false)
				.map(k -> RiakUtils.createLocation(nameSpace,k))
				.map(l -> new FetchValue.Builder(l).build())
				.map(f -> 		
					{
						try {
							return client.execute(f);
						} catch (ExecutionException | InterruptedException e) {
							throw new DianaRiakException(e.getMessage(),e);
						}
					}
				)
				.filter(Response::hasValues)
				.map(r -> {
					
					try {
						return r.getValue(String.class);
					} catch (UnresolvedConflictException e) {
						throw new DianaRiakException(e.getMessage(),e);
					}
					
				})
				.filter(StringUtils::isNotBlank).map(v -> RiakValue.of(gson, v))
				.collect(toList());
	}
	

	@Override
	public <K> void remove(K key) throws NullPointerException {

		DeleteValue deleteValue = createDeleteValue(nameSpace,key);
		
		try {
			client.execute(deleteValue);
		} catch (ExecutionException | InterruptedException e) {
			throw new DianaRiakException(e.getMessage(),e);
		}
	}

	@Override
	public <K> void remove(Iterable<K> keys) throws NullPointerException {
		StreamSupport.stream(keys.spliterator(),false).forEach(this::remove);
	}

	@Override
	public void close() {
		client.shutdown();
	}
}
