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

	private final RiakCluster cluster;

	RiakKeyValueEntityManagerFactory(RiakCluster cluster) {
		this.cluster = cluster;
	}

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
