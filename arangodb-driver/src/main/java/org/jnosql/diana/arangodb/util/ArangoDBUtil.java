package org.jnosql.diana.arangodb.util;


import com.arangodb.ArangoDB;
import org.jnosql.diana.arangodb.document.ArangoDBException;

import java.util.Collection;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ArangoDBUtil {

    private static final Logger LOGGER = Logger.getLogger(ArangoDBUtil.class.getName());

    private ArangoDBUtil() {
    }


    public static void checkDatabase(String database, ArangoDB arangoDB) {
        Objects.requireNonNull(database, "database is required");
        try {
            Collection<String> databases = arangoDB.getDatabases();
            if (!databases.contains(database)) {
                arangoDB.createDatabase(database);
            }
        } catch (ArangoDBException e) {
            LOGGER.log(Level.WARNING, "Failed to create database: " + database, e);
        }
    }
}
