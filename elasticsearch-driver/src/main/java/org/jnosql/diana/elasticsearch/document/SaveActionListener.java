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
