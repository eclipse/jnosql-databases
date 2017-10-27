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


import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentDeleteEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import com.arangodb.entity.MultiDocumentEntity;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.checkCondition;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.getBaseDocument;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.toEntity;

/**
 * The ArandoDB implementation of {@link DocumentCollectionManagerAsync}. It does not support to TTL methods:
 * <p>{@link DocumentCollectionManagerAsync#insert(DocumentEntity, Duration)}</p>
 * <p>{@link DocumentCollectionManagerAsync#insert(DocumentEntity, Duration, Consumer)}</p>
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
