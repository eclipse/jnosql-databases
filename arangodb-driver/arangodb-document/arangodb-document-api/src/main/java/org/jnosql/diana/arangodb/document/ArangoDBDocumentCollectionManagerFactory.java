package org.jnosql.diana.arangodb.document;


import com.arangodb.ArangoDB;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.util.Objects;

public class ArangoDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<ArangoDBDocumentCollectionManager> {


    private final ArangoDB arangoDB;

    ArangoDBDocumentCollectionManagerFactory(ArangoDB arangoDB) {
        this.arangoDB = arangoDB;
    }

    @Override
    public ArangoDBDocumentCollectionManager getDocumentEntityManager(String database) {
        Objects.requireNonNull(database, "database is required");
        Boolean database1 = arangoDB.createDatabase(database);
        return new ArangoDBDocumentCollectionManager(database, arangoDB);
    }

    @Override
    public void close() {
        arangoDB.shutdown();
    }
}
