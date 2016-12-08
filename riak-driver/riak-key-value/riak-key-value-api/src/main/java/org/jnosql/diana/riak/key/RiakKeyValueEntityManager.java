package org.jnosql.diana.riak.key;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Builder;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
public class RiakKeyValueEntityManager implements BucketManager {


	private final RiakClient client;
	
	private final Namespace nameSpace;
	
	
	public RiakKeyValueEntityManager(RiakClient client, Namespace nameSpace) {
		super();
		this.client = client;
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


		StoreValue storeValue = createStoreValue(entity.getKey(),entity.getValue(),ttl);
		try {
			client.execute(storeValue);
		} catch (ExecutionException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		
		 Location location = new Location(nameSpace,key.toString());
         FetchValue fv = new FetchValue.Builder(location).build();
         try {
			FetchValue.Response response = client.execute(fv);
			//Pojo myObject = response.getValue(Pojo.class);
//	        System.out.println(myObject.foo);
		} catch (ExecutionException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
		//StreamSupport.stream(keys.spliterator(),false).forEach(action);
		return null;
	}

	@Override
	public <K> void remove(K key) throws NullPointerException {
		// TODO Auto-generated method stub
		Location location = new Location(nameSpace, key.toString());
		DeleteValue dv = new DeleteValue.Builder(location).build();
		try {
			client.execute(dv);
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

	private <K,V> StoreValue createStoreValue(K key, V value, Duration ttl){
		
		Objects.requireNonNull(value, "Value is required");
        Objects.requireNonNull(key, "key is required");
		
		Location location = new Location(nameSpace, key.toString());
        Builder builder = new StoreValue.Builder(key).withLocation(location);
        
        if(!ttl.isZero())
        	builder = builder.withTimeout(Math.toIntExact(ttl.getSeconds()));
        
        
        return builder.build();
        
	}
}
