/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Apache License v2.0 which accompanies this distribution.
 *   The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *   and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 *   You may elect to redistribute this code under either of these licenses.
 *
 *   Contributors:
 *
 *   Otavio Santana
 */

/**
 * ArangoDB is a native NoSQL multi-model database system developed by triAGENS GmbH. In a book published in 2015,
 * it was referred to as the most popular NoSQL database available that had an open-source license.
 * It has also been referred to as a universal database. Its creators refer to it as a "native multi-model" database to
 * indicate that it was designed specifically to allow key/value, document, and graph data to be stored together and
 * queried with a common language.
 * ArangoDB provides scalable, highly efficient queries when working with graph data.
 * The database uses JSON as a default storage format, but internally it uses ArangoDB's VelocyPack –
 * a fast and compact binary format for serialization and storage. ArangoDB can natively store a nested JSON object
 * as a data entry inside a collection. Therefore, there is no need to disassemble the resulting JSON objects.
 * Thus, the stored data would simply inherit the tree structure of the XML data.
 * ArangoDB works in a distributed cluster unlike some other existing graph databases and it is the first DBMS being
 * certified for the Datacenter Operating System (DC/OS). DC/OS allows the user to deploy ArangoDB
 * on most existing ecosystems: Amazon Web Services (AWS), Google Compute Engine and Microsoft Azure.
 * Moreover, it provides single-click deployment for the user's cluster.
 */
package org.eclipse.jnosql.mapping.hazelcast.keyvalue;