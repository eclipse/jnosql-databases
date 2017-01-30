package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.client.Client;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public class ElasticsearchDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {

    private final Client client;
    private final String database;

    ElasticsearchDocumentCollectionManagerAsync(Client client, String database) {

        this.client = client;
        this.database = database;
    }

    @Override
    public void save(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void save(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void save(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void save(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void delete(DocumentQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void delete(DocumentQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void find(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {

    }

    @Override
    public void close() {

    }
}
