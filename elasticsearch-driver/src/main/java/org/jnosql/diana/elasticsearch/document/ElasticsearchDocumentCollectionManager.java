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
package org.jnosql.diana.elasticsearch.document;


import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;

public class ElasticsearchDocumentCollectionManager implements DocumentCollectionManager {
    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        return null;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        return null;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) throws NullPointerException {
        return null;
    }

    @Override
    public void delete(DocumentQuery query) throws NullPointerException {

    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {
        return null;
    }

    @Override
    public void close() {

    }
}
