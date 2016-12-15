package org.jnosql.diana.riak.key;

import java.time.Duration;
import java.util.Objects;

import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Builder;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

public class RiakUtils {

	public static <K,V> StoreValue createStoreValue(K key, V value, Namespace namespace ,Duration ttl){
		
		Objects.requireNonNull(value, "Value is required");
        Objects.requireNonNull(key, "key is required");
		
		Location location = createLocation(namespace, key);
        Builder builder = new StoreValue.Builder(key).withLocation(location);
        
        if(!ttl.isZero())
        	builder = builder.withTimeout(Math.toIntExact(ttl.getSeconds()));
        
        return builder.build();
	}
	
	public static <K> Location createLocation (Namespace namespace, K key){
		
		Objects.requireNonNull(namespace, "Namespace is required");
        Objects.requireNonNull(key, "key is required");
		
        return new Location(namespace, key.toString());
	}
	
	public static <K> FetchValue createFetchValue(Namespace namespace, K key){
		
		Location location = createLocation(namespace, key);
		return new FetchValue.Builder(location).build();
	}
	
	public static <K> DeleteValue createDeleteValue(Namespace namespace, K key){

		Location location = createLocation(namespace, key);
		return new DeleteValue.Builder(location).build();
	}
}
