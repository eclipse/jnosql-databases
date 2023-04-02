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
package org.eclipse.jnosql.mapping.orientdb.document;


import jakarta.nosql.document.DocumentTemplate;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.orientdb.document.OrientDBLiveCallback;
import org.eclipse.jnosql.mapping.document.JNoSQLDocumentTemplate;

import java.util.Map;
import java.util.stream.Stream;

/**
 * A {@link DocumentTemplate} to orientdb
 */
public interface OrientDBTemplate extends JNoSQLDocumentTemplate {

    /**
     * Find using OrientDB native query
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    <T> Stream<T> sql(String query, Object... params);

    /**
     * Find using OrientDB native query with map params
     *
     * @param query  the query
     * @param params the params
     * @return the query result
     * @throws NullPointerException when either query or params are null
     */
    <T> Stream<T> sql(String query, Map<String, Object> params);
    /**
     * Execute live query
     *
     * @param query    the query
     * @param callBacks callbacks for each operation
     * @throws NullPointerException when both query and callBack are null
     */
    <T> void live(DocumentQuery query, OrientDBLiveCallback<T> callBacks);

    /**
     * Execute live query
     *
     * @param query    the string query, you must add "live"
     * @param callBacks callbacks for each operation
     * @param params   the params
     * @throws NullPointerException when both query, callBack are null
     */
    <T> void live(String query, OrientDBLiveCallback<T> callBacks, Object... params);
}
