/*
 * Copyright 2017 Otavio Santana and others
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

/**
 * <p>ArangoDB is a NoSQL multi-model database developed by triAGENS GmbH. It has been referred to as the most popular NoSQL database available that has an open source license. It has also been referred to as a universal database. Its creators refer to it as a "native multi-model" database to indicate that it was designed specifically to allow key/value, document, and graph data to be stored together and queried with a common language.</p>
 * <p>ArangoDB has a low resource consumption and high performance, as shown in the latest open-source NoSQL performance test.</p>
 * <p>ArangoDB provides scalable, highly efficient queries when working with graph data. The database uses JSON as a default storage format, but internally it uses ArangoDB's VelocyPack - a fast and compact binary format for serialization and storage. ArangoDB can natively store a nested JSON object as a data entry inside a collection. Therefore, there is no need to disassemble the resulting JSON objects. Thus, the stored data would simply inherit the tree structure of the XML data.</p>
 * <p>ArangoDB works in a distributed cluster unlike some other existing graph databases and it is the first DBMS being certified for the Distributed Cluster Operating System (DC/OS).[12] DC/OS allows to deploy ArangoDB on most of the existing ecosystems: Amazon Web Services (AWS), Google Compute Engine and Microsoft Azure. Moreover, it also provides you a single click deployment in your own cluster.</p>
 * <p>ArangoDB provides native integration of the JavaScript microservices directly on top of the DBMS using the Foxx framework, which is an analogue of the multithreaded NodeJS.</p>
 * <p>The database has both AQL query language and provides GraphQL to write flexible native web services directly on top of the DBMS</p>
 */
package org.jnosql.diana.arangodb;