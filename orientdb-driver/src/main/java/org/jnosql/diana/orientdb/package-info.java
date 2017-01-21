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
/**
 * OrientDB is an open source NoSQL database management system written in Java. It is a multi-model database,
 * supporting graph, document, key/value, and object models, but the relationships are managed as in graph databases
 * with direct connections between records. It supports schema-less, schema-full and schema-mixed modes.
 * It has a strong security profiling system based on users and roles and supports querying with Gremlin
 * along with SQL extended for graph traversal. OrientDB uses several indexing mechanisms based
 * on B-tree and Extendible hashing, the last one is known as "hash index", there are plans to implement
 * LSM-tree and Fractal tree index based indexes. Each record has Surrogate key which indicates position
 * of record inside of Array list , links between records are stored either as single value of record's
 * position stored inside of referrer or as B-tree of record positions (so-called record IDs or RIDs)
 * which allows fast traversal (with O(1) complexity) of one-to-many relationships and fast addition/removal
 * of new links. OrientDB is the second most popular graph database according to the DB-Engines graph database ranking.
 */
package org.jnosql.diana.orientdb;