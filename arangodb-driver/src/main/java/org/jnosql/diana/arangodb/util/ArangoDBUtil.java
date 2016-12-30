package org.jnosql.diana.arangodb.util;


import com.arangodb.ArangoDB;
import com.arangodb.entity.CollectionEntity;
import org.jnosql.diana.arangodb.document.ArangoDBException;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

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

    public static void checkCollection(String bucketName, ArangoDB arangoDB, String namespace) {
        checkDatabase(bucketName, arangoDB);
        List<String> collections = arangoDB.db(bucketName)
                .getCollections().stream()
                .map(CollectionEntity::getName)
                .collect(toList());
        if (!collections.contains(namespace)) {
            arangoDB.db(bucketName).createCollection(namespace);
        }
    }
}
