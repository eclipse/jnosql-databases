package org.jnosql.diana.riak.key;


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
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.google.gson.Gson;

public class RiakKeyValueEntityManager implements BucketManager {


	private final RiakClient client;
	
	private final Gson gson;
	
	private final Namespace nameSpace;
	
	public RiakKeyValueEntityManager(RiakClient client, Gson gson, Namespace nameSpace) {
		super();
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

		StoreValue storeValue = RiakUtils.createStoreValue(key,value,nameSpace,ttl);
		try {
			client.execute(storeValue);
		} catch (ExecutionException e) {
			throw new DianaRiakException(e.getMessage(),e);
		} catch (InterruptedException e) {
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
		
		//terminar
		//if(StringUtils.isNoneBlank(valueFetch))
		
		Location location = RiakUtils.createLocation(nameSpace, key);
        FetchValue fv = new FetchValue.Builder(location).build();
		try {
			FetchValue.Response response = client.execute(fv);
			String valueFetch = response.getValue(String.class);
			if(StringUtils.isNoneBlank(valueFetch))
				 return Optional.of(RiakValue.of(gson,valueFetch));
	    
		} catch (UnresolvedConflictException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	     
		return Optional.empty();
	}

	@Override
	public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
		
		StreamSupport.stream(keys.spliterator(),false)
			.map(k -> RiakUtils.createLocation(nameSpace,k))
			.map(l -> new FetchValue.Builder(l).build())
			.map(f -> {
				
				try {
					return client.execute(f);
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return null;
			})
			.map(r -> {
				try {
					return r.getValue(String.class);
				} catch (UnresolvedConflictException e) {
					e.printStackTrace();
				}
				return null;
			})
			.filter(StringUtils::isNotBlank).map(v -> RiakValue.of(gson, v))
			.collect(toList());
			
		
		return null;
	}

	@Override
	public <K> void remove(K key) throws NullPointerException {

		Location location = RiakUtils.createLocation(nameSpace, key.toString());
		DeleteValue dv = new DeleteValue.Builder(location).build();
		try {
			client.execute(dv);
		} catch (ExecutionException e) {
			throw new DianaRiakException(e.getMessage(),e);
		} catch (InterruptedException e) {
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
