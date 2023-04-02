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
package org.eclipse.jnosql.databases.solr.communication;

import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The solr implementation to {@link DocumentManager} that does not support TTL methods
 * <p>{@link DefaultSolrDocumentManager#insert(DocumentEntity, Duration)}</p>
 */
public interface SolrDocumentManager extends DocumentManager {

    /**
     * Executes a Solr native query
     *
     * @param query the query
     * @return the result
     * @throws NullPointerException when query is null
     */
    List<DocumentEntity> solr(String query);

    /**
     * Executes a Solr native query with params.
     *
     * @param query  the query
     * @param params the params
     * @return the result
     * @throws NullPointerException when there is null parameter
     */
    List<DocumentEntity> solr(String query, Map<String, ? extends Object> params);
}
