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
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.CouchbaseCluster;
import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.couchbase.CouchbaseConfiguration;
import org.jnosql.diana.couchbase.key.CouchbaseBucketManagerFactory;

/**
 * The couchbase implementation of {@link UnaryDocumentConfiguration} that returns
 * {@link CouhbaseDocumentCollectionManagerFactory}.
 * <p>couchbase-host-: the prefix to add a new host</p>
 * <p>couchbase-user: the user</p>
 * <p>couchbase-password: the password</p>
 */
public class CouchbaseDocumentConfiguration extends CouchbaseConfiguration
        implements UnaryDocumentConfiguration<CouhbaseDocumentCollectionManagerFactory> {

    @Override
    public CouhbaseDocumentCollectionManagerFactory get() throws UnsupportedOperationException {
        return new CouhbaseDocumentCollectionManagerFactory(CouchbaseCluster.create(nodes), user, password);
    }

    @Override
    public CouhbaseDocumentCollectionManagerFactory getAsync() throws UnsupportedOperationException {
        return new CouhbaseDocumentCollectionManagerFactory(CouchbaseCluster.create(nodes), user, password);
    }
}
