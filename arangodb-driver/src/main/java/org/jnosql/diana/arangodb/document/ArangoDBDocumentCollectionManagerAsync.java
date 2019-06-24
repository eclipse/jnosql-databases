/*
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
 */
package org.jnosql.diana.arangodb.document;


import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.document.DocumentCollectionManagerAsync;
import jakarta.nosql.document.DocumentEntity;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The ArandoDB implementation of {@link DocumentCollectionManagerAsync}. It does not support to TTL methods:
 * <p>{@link DocumentCollectionManagerAsync#insert(DocumentEntity, java.time.Duration)}</p>
 * <p>{@link DocumentCollectionManagerAsync#insert(DocumentEntity, java.time.Duration, Consumer)}</p>
 */
public interface ArangoDBDocumentCollectionManagerAsync extends DocumentCollectionManagerAsync {

    /**
     * Executes AQL, finds {@link DocumentEntity} from select asynchronously
     * <p>FOR u IN users FILTER u.status == @status RETURN u </p>
     *
     * @param query    the query
     * @param values   the named queries
     * @param callBack the callback, when the process is finished will call this instance returning
     *                 the result of select within parameters
     * @throws ExecuteAsyncQueryException    when there is a async error
     * @throws UnsupportedOperationException when the database does not support this feature
     * @throws NullPointerException          when either select or callback are null
     */
    void aql(String query, Map<String, Object> values, Consumer<List<DocumentEntity>> callBack) throws
            ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException;


}
