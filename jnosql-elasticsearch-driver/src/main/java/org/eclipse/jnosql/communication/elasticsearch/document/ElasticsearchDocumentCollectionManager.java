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
package org.eclipse.jnosql.communication.elasticsearch.document;


import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentEntity;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.stream.Stream;

/**
 * The ES implementation of {@link DocumentCollectionManager}
 */
public interface ElasticsearchDocumentCollectionManager extends DocumentCollectionManager {

    /**
     * Find entities from {@link QueryBuilder}
     *
     * @param query the query
     * @return the objects from query
     * @throws NullPointerException when query is null
     */
     Stream<DocumentEntity> search(QueryBuilder query) throws NullPointerException;


}
