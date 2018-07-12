/*
 *
 *  Copyright (c) 2017 Otávio Santana and others
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
 *
 */
package org.jnosql.diana.couchdb.document;

import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;

import java.util.function.Consumer;

public interface CouchDBDocumentCollectionManagerAsync extends DocumentCollectionManagerAsync {

    /**
     * Returns the number of elements of database
     *
     * @param callback the callback with the response
     * @return the number of elements
     * @throws NullPointerException when there is null parameter
     */
    long count(Consumer<Long> callback);
}
