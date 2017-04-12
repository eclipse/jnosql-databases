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
package org.google.storage.driver.key;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * The riak implementation to {@link BucketManagerFactory} that returns {@link RiakKeyValueEntityManager}
 * This implementation just has support to {@link RiakKeyValueEntityManagerFactory#getBucketManager(String)}
 * So, these metdhos will returns {@link UnsupportedOperationException}
 * <p>{@link BucketManagerFactory#getList(String, Class)}</p>
 * <p>{@link BucketManagerFactory#getSet(String, Class)}</p>
 * <p>{@link BucketManagerFactory#getQueue(String, Class)}</p>
 * <p>{@link BucketManagerFactory#getMap(String, Class, Class)}</p>
 */

public class GoogleStorageKeyValueEntityManagerFactory implements BucketManagerFactory<GoogleStorageKeyValueEntityManager> {

	private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();
	private final Storage storage;
	
	public GoogleStorageKeyValueEntityManagerFactory(Storage storage) {
		this.storage = storage;
	}

	@Override
	public GoogleStorageKeyValueEntityManager getBucketManager(String bucketName)
			throws UnsupportedOperationException, NullPointerException {
		
		return new GoogleStorageKeyValueEntityManager(storage, bucketName);
	}

	@Override
	public <T> List<T> getList(String bucketName, Class<T> clazz)
			throws UnsupportedOperationException, NullPointerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Set<T> getSet(String bucketName, Class<T> clazz)
			throws UnsupportedOperationException, NullPointerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Queue<T> getQueue(String bucketName, Class<T> clazz)
			throws UnsupportedOperationException, NullPointerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue)
			throws UnsupportedOperationException, NullPointerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
