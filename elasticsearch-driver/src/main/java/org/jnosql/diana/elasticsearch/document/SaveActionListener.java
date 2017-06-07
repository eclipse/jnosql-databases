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
package org.jnosql.diana.elasticsearch.document;

import org.elasticsearch.action.index.IndexResponse;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.function.Consumer;

class SaveActionListener implements org.elasticsearch.action.ActionListener<IndexResponse> {

    private final Consumer<DocumentEntity> callBack;

    private final DocumentEntity entity;

    SaveActionListener(Consumer<DocumentEntity> callBack, DocumentEntity entity) {
        this.callBack = callBack;
        this.entity = entity;
    }

    @Override
    public void onResponse(IndexResponse indexResponse) {
        callBack.accept(entity);
    }

    @Override
    public void onFailure(Exception e) {
        throw new ExecuteAsyncQueryException("An error when execute async elasticsearch query", e);
    }
}
