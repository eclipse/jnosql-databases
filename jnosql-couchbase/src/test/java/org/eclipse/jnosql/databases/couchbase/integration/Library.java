
/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 *
 */
package org.eclipse.jnosql.databases.couchbase.integration;

import jakarta.data.repository.Repository;
import org.eclipse.jnosql.databases.couchbase.mapping.CouchbaseRepository;

import java.util.stream.Stream;

@Repository
public interface Library extends CouchbaseRepository<Book, String> {

    Stream<Book> findByEditionLessThan(Integer edition);

    Stream<Book> findByEditionGreaterThan(Integer edition);

    Stream<Book> findByTitleLike(String title);

}
