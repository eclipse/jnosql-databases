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


import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.util.List;
import java.util.Map;

/**
 * The orientdb implementation to {@link DocumentCollectionManager}, this implementation
 * does not support TTL.
 * <p>{@link OrientDBDocumentCollectionManager#insert(DocumentEntity, java.time.Duration)}</p>
 * Also this implementation has support SQL query and also live query.
 * <p>{@link OrientDBDocumentCollectionManager#sql(String, Object...)}</p>
 */
public interface OrientDBDocumentCollectionManager extends DocumentCollectionManager {
    /**
     * Find using query
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    List<DocumentEntity> sql(String query, Object... params);

    /**
     * Find using query
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    List<DocumentEntity> sql(String query, Map<String, Object> params);

    /**
     * Execute live query
     *
     * @param query     the query
     * @param callbacks Callbacks for create, update and delete operations
     * @throws NullPointerException when both query and callBack are null
     */
    void live(DocumentQuery query, OrientDBLiveCallback<DocumentEntity> callbacks);

    /**
     * Execute live query
     *
     * @param query     the query
     * @param callbacks Callbacks for create, update and delete operations
     * @throws NullPointerException when both query and callBack are null
     */
    void live(String query, OrientDBLiveCallback<DocumentEntity> callbacks, Object... params);
}
