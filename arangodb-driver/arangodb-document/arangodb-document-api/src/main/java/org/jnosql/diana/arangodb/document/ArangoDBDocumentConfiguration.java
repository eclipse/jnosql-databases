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
import org.jnosql.diana.api.document.DocumentConfiguration;

public class ArangoDBDocumentConfiguration implements DocumentConfiguration<ArangoDBDocumentCollectionManagerFactory> {

    private ArangoDB.Builder builder = new ArangoDB.Builder();


    public void host(String host) {
        builder.host(host);
    }

    public void port(int port) {
        builder.port(port);
    }

    public void timeout(int timeout) {
        builder.timeout(timeout);
    }

    public void user(String user) {
        builder.user(user);
    }

    public void password(String password) {
        builder.password(password);
    }


    @Override
    public ArangoDBDocumentCollectionManagerFactory getManagerFactory() {
        return new ArangoDBDocumentCollectionManagerFactory(builder.build());
    }
}
