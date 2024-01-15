package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import org.eclipse.jnosql.communication.keyvalue.BucketManager;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

final class OracleBucketManagerFactory implements BucketManagerFactory {

    private final NoSQLHandle bucketManager;

    OracleBucketManagerFactory(NoSQLHandle bucketManager) {
        this.bucketManager = bucketManager;
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> type) {
        throw new UnsupportedOperationException("Oracle NoSQL does not support list");
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> type) {
        throw new UnsupportedOperationException("Oracle NoSQL does not support set");
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> type) {
        throw new UnsupportedOperationException("Oracle NoSQL does not support queue");
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyType, Class<V> valueType) {
        throw new UnsupportedOperationException("Oracle NoSQL does not support map");
    }

    @Override
    public BucketManager apply(String bucketName) {
        Objects.requireNonNull(bucketName, "bucketName is required");
        return null;
    }

    @Override
    public void close() {
        bucketManager.close();
    }


}
