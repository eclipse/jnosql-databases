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

import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.List;
import java.util.Map;

/**
 * The ArangoDB implementation of {@link DocumentCollectionManager} it does not support to TTL methods:
 * <p>{@link DocumentCollectionManager#insert(DocumentEntity)}</p>
 */
public interface ArangoDBDocumentCollectionManager extends DocumentCollectionManager {


    /**
     * Executes ArangoDB query language, AQL.
     * <p>FOR u IN users FILTER u.status == @status RETURN u </p>
     *
     * @param query  the query
     * @param values the named queries
     * @return the query result
     * @throws NullPointerException when either query or values are null
     */
    List<DocumentEntity> aql(String query, Map<String, Object> values) throws NullPointerException;

}
