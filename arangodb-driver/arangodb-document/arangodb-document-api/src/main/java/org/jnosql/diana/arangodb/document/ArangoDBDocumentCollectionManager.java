package org.jnosql.diana.arangodb.document;

import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;


public class ArangoDBDocumentCollectionManager implements DocumentCollectionManager {


    private static final String KEY_NAME = "";
    private static final Predicate<Document> FIND_KEY_DOCUMENT = d -> KEY_NAME.equals(d.getName());

    private final String database;

    private final ArangoDB arangoDB;
    private final ValueWriter writerField = ValueWriterDecorator.getInstance();

    ArangoDBDocumentCollectionManager(String database, ArangoDB arangoDB) {
        this.database = database;
        this.arangoDB = arangoDB;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        String collectionName = entity.getName();
        arangoDB.db(database).createCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        arangoDB.db(database).collection(collectionName).insertDocument(baseDocument);
        return entity;
    }



    @Override
    public void saveAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }


    @Override
    public void saveAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        String collectionName = entity.getName();
        arangoDB.db(database).createCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        arangoDB.db(database).collection(collectionName).updateDocument(baseDocument.getKey(), baseDocument);
        return entity;
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
        arangoDB.shutdown();
    }

    private Object convert(Value value) {
        Object val = value.get();
        if (writerField.isCompatible(val.getClass())) {
            return writerField.write(val);
        }
        return val;
    }

    private BaseDocument getBaseDocument(DocumentEntity entity) {
        BaseDocument baseDocument = new BaseDocument();

        baseDocument.setKey(entity.getDocuments().stream()
                .filter(FIND_KEY_DOCUMENT).findFirst()
                .map(Document::getName)
                .orElseThrow(() -> new ArangoDBException("The entity must have a entity key")));
        entity.getDocuments().stream()
                .filter(FIND_KEY_DOCUMENT.negate())
                .forEach(d -> baseDocument.addAttribute(d.getName(),
                        convert(d.getValue())));
        return baseDocument;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
       throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }
}
