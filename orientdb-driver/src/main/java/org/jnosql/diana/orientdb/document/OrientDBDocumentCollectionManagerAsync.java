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


import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.List;
import java.util.function.Consumer;

/**
 * The OrientDB implementation to {@link DocumentCollectionManagerAsync} this method does not support TTL method:
 * <p> {@link OrientDBDocumentCollectionManagerAsync#insert(DocumentEntity, java.time.Duration)}</p>
 * <p>{@link OrientDBDocumentCollectionManagerAsync#insert(DocumentEntity, java.time.Duration, Consumer)}</p>
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
