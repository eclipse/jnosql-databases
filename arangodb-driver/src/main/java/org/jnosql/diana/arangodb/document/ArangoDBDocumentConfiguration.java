/*
 * Copyright 2017 Eclipse Foundation
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

import org.jnosql.diana.api.document.UnaryDocumentConfiguration;
import org.jnosql.diana.arangodb.ArangoDBConfiguration;

/**
 * The implementation of {@link UnaryDocumentConfiguration} that returns {@link ArangoDBDocumentCollectionManagerFactory}.
 * It tries to read the configuration properties from arangodb.properties file.
 * @see ArangoDBConfiguration
 *
 */
public class ArangoDBDocumentConfiguration extends ArangoDBConfiguration
        implements UnaryDocumentConfiguration<ArangoDBDocumentCollectionManagerFactory> {


    @Override
    public ArangoDBDocumentCollectionManagerFactory get() throws UnsupportedOperationException {
        return new ArangoDBDocumentCollectionManagerFactory(builder.build(), builderAsync.build());
    }

    @Override
    public ArangoDBDocumentCollectionManagerFactory getAsync() throws UnsupportedOperationException {
        return new ArangoDBDocumentCollectionManagerFactory(builder.build(), builderAsync.build());
    }
}
