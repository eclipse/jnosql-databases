package org.jnosql.diana.arangodb.document;

import com.arangodb.ArangoDB;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;


public class ArangoDBDocumentCollectionManager implements DocumentCollectionManager {

    private final String database;
    private final ArangoDB arangoDB;

    ArangoDBDocumentCollectionManager(String database, ArangoDB arangoDB) {
        this.database = database;
        this.arangoDB = arangoDB;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        return null;
    }

    @Override
    public void saveAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        return null;
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void saveAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return null;
    }

    @Override
    public void updateAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void updateAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void delete(DocumentQuery query) {

    }

    @Override
    public void deleteAsync(DocumentQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void deleteAsync(DocumentQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {
        return null;
    }

    @Override
    public void findAsync(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void close() {

    }
}
