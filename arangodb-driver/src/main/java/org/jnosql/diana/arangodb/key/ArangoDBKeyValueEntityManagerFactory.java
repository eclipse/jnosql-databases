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
package org.jnosql.diana.arangodb.key;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.arangodb.util.ArangoDBUtil;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class ArangoDBKeyValueEntityManagerFactory implements BucketManagerFactory<ArangoDBValueEntityManager> {



    private final ArangoDB arangoDB;
    private final ArangoDBAsync arangoDBAsync;

    ArangoDBKeyValueEntityManagerFactory(ArangoDB arangoDB, ArangoDBAsync arangoDBAsync) {
        this.arangoDB = arangoDB;
        this.arangoDBAsync = arangoDBAsync;
    }

    @Override
    public ArangoDBValueEntityManager getBucketManager(String bucketName) throws UnsupportedOperationException {
        ArangoDBUtil.checkDatabase(bucketName, arangoDB);
        return new ArangoDBValueEntityManager(arangoDB, arangoDBAsync);
    }

    @Override
    public Map getMap(String bucketName, Class keyValue, Class valueValue) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The ArangoDB does not support getMap method");
    }

    @Override
    public Queue getQueue(String bucketName, Class clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The ArangoDB does not support getQueue method");
    }

    @Override
    public Set getSet(String bucketName, Class clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The ArangoDB does not support getSet method");
    }

    @Override
    public List getList(String bucketName, Class clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The ArangoDB does not support getList method");
    }

    @Override
    public void close() {
        arangoDB.shutdown();
        arangoDBAsync.shutdown();
    }


}
