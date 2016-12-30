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
package org.jnosql.diana.arangodb.document;


import com.arangodb.ArangoDB;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.util.Objects;

public class ArangoDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<ArangoDBDocumentCollectionManager> {


    private final ArangoDB arangoDB;

    ArangoDBDocumentCollectionManagerFactory(ArangoDB arangoDB) {
        this.arangoDB = arangoDB;
    }

    @Override
    public ArangoDBDocumentCollectionManager getDocumentEntityManager(String database) {
        Objects.requireNonNull(database, "database is required");
        Boolean database1 = arangoDB.createDatabase(database);
        return new ArangoDBDocumentCollectionManager(database, arangoDB);
    }

    @Override
    public void close() {
        arangoDB.shutdown();
    }
}
