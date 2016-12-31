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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public class OrientDBDocumentCollectionManager implements DocumentCollectionManager {

    private final OPartitionedDatabasePool pool;

    OrientDBDocumentCollectionManager(OPartitionedDatabasePool pool) {
        this.pool = pool;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        return null;
    }

    @Override
    public void saveAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        return null;
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void saveAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return null;
    }

    @Override
    public void updateAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void updateAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void delete(DocumentQuery query) {

    }

    @Override
    public void deleteAsync(DocumentQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void deleteAsync(DocumentQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {
        return null;
    }

    @Override
    public void findAsync(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void close() {

    }
}
