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
package org.eclipse.jnosql.mapping.elasticsearch.document;


import co.elastic.clients.elasticsearch.core.SearchRequest;
import org.eclipse.jnosql.mapping.document.JNoSQLDocumentTemplate;

import java.util.stream.Stream;

/**
 * A {@link JNoSQLDocumentTemplate} to elasticsearch
 */
public interface ElasticsearchTemplate extends JNoSQLDocumentTemplate {

    /**
     * Find entities from {@link SearchRequest}
     *
     * @param query the query
     * @return the objects from query
     * @throws NullPointerException when query is null
     */
    <T> Stream<T> search(SearchRequest query);
}
