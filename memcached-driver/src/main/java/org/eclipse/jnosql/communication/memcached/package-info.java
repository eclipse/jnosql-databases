/*
 *  Copyright (c) 2019 Otávio Santana and others
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
 * Memcached (pronunciation: mem-cashed, mem-cash-dee) is a general-purpose distributed memory caching system.
 * It is often used to speed up dynamic database-driven websites by caching data and objects in RAM to reduce
 * the number of times an external data source (such as a database or API) must be read. Memcached is free and
 * open-source software, licensed under the Revised BSD license. Memcached runs on Unix-like operating systems
 * (at least Linux and OS X) and on Microsoft Windows. It depends on the libevent library.
 * Memcached's APIs provide a very large hash table distributed across multiple machines.
 * When the table is full, subsequent inserts cause older data to be purged in least recently used (LRU) order.
 */
package org.eclipse.jnosql.communication.memcached;