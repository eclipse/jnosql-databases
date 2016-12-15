package org.jnosql.diana.riak.key;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.jnosql.diana.api.key.BucketManagerFactory;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.query.Namespace;
import com.google.gson.Gson;

public class RiakKeyValueEntityManagerFactory implements BucketManagerFactory<RiakKeyValueEntityManager> {

	private RiakCluster cluster;
	
	@Override
	public RiakKeyValueEntityManager getBucketManager(String bucketName) throws UnsupportedOperationException {
		
		cluster.start();
		RiakClient riakClient = new RiakClient(cluster);
		Namespace quotesBucket = new Namespace(bucketName);

		return new RiakKeyValueEntityManager(riakClient, new Gson(),quotesBucket);
	}

	@Override
	public <T> List<T> getList(String bucketName, Class<T> clazz) throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Set<T> getSet(String bucketName, Class<T> clazz) throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue)
			throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		cluster.shutdown();
	}

}
