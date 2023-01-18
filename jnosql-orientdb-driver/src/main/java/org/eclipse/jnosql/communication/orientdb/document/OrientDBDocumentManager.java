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
package org.eclipse.jnosql.communication.orientdb.document;


import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.util.Map;
import java.util.stream.Stream;

/**
 * The orientdb implementation to {@link DocumentManager}, this implementation
 * does not support TTL.
 * <p>{@link OrientDBDocumentManager#insert(DocumentEntity, java.time.Duration)}</p>
 * Also this implementation has support SQL query and also live query.
 * <p>{@link OrientDBDocumentManager#sql(String, Object...)}</p>
 */
public interface OrientDBDocumentManager extends DocumentManager {
    /**
     * Find using query
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    Stream<DocumentEntity> sql(String query, Object... params);

    /**
     * Find using query
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    Stream<DocumentEntity> sql(String query, Map<String, Object> params);

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
