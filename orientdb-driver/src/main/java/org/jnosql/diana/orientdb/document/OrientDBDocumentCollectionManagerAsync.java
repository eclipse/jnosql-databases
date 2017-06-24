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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE.ASYNCHRONOUS;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.orientdb.document.OSQLQueryFactory.toAsync;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;

/**
 * The OrientDB implementation to {@link DocumentCollectionManagerAsync} this method does not support TTL method:
 * <p> {@link OrientDBDocumentCollectionManagerAsync#insert(DocumentEntity, Duration)}</p>
 * <p>{@link OrientDBDocumentCollectionManagerAsync#insert(DocumentEntity, Duration, Consumer)}</p>
 * Also has supports to query:
 * <p>{@link OrientDBDocumentCollectionManagerAsync#find(String, Consumer, Object...)}</p>
 */
public interface OrientDBDocumentCollectionManagerAsync extends DocumentCollectionManagerAsync {


    /**
     * Find async from Query
     *
     * @param query    the query
     * @param callBack the callback
     * @param params   the params
     * @throws NullPointerException when there any parameter null
     */
    void find(String query, Consumer<List<DocumentEntity>> callBack, Object... params) throws NullPointerException;
}
