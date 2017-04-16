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

import com.google.cloud.storage.Storage;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * The google stroage implementation of {@link BucketManagerFactory}
 */
public class GoogleStorageKeyValueEntityManagerFactory implements BucketManagerFactory<GoogleStorageKeyValueEntityManager> {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();
    private final Storage storage;

    GoogleStorageKeyValueEntityManagerFactory(Storage storage) {
        this.storage = storage;
    }

    @Override
    public GoogleStorageKeyValueEntityManager getBucketManager(String bucketName)
            throws UnsupportedOperationException, NullPointerException {

        return new GoogleStorageKeyValueEntityManager(storage, bucketName, PROVDER);
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz)
            throws UnsupportedOperationException, NullPointerException {
        throw new UnsupportedOperationException("Google does not support List structure");
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz)
            throws UnsupportedOperationException, NullPointerException {
        throw new UnsupportedOperationException("Google does not support List structure");
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz)
            throws UnsupportedOperationException, NullPointerException {
        throw new UnsupportedOperationException("Google does not support List structure");
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue)
            throws UnsupportedOperationException, NullPointerException {
        throw new UnsupportedOperationException("Google does not support List structure");
    }

    @Override
    public void close() {
    }

}
