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

package org.jnosql.diana.ravendb.document;

import com.mongodb.MongoClient;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.ravendb.document.MongoDBDocumentCollectionManagerAsyncFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RavenDBDocumentCollectionManagerFactoryTest {

    private static RavenDBDocumentConfiguration configuration;

    @BeforeAll
    public static void setUp() throws IOException {
        configuration = new RavenDBDocumentConfiguration();
        MongoDbHelper.startMongoDb();
    }

    @Test
    public void shouldCreateEntityManager() {
        RavenDBDocumentCollectionManagerFactory mongoDBFactory = configuration.get();
        assertNotNull(mongoDBFactory.get("database"));
    }

    @Test
    public void shouldReturnNPEWhenSettingsIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.get((Settings) null));
    }

    @Test
    public void shouldReturnNPEWhenMapSettingsIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.get((Map<String, String>) null));
    }

    @Test
    public void shouldReturnNPEWhenMongoClientIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.get((MongoClient) null));
    }


    @Test
    public void shouldCreateEntityManagerAsync() {
        MongoDBDocumentCollectionManagerAsyncFactory mongoDBFactory = configuration.getAsync();
        assertNotNull(mongoDBFactory.getAsync("database"));
    }

    @Test
    public void shouldReturnNPEWhenSettingOnAsyncsIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.getAsync((Settings) null));
    }

    @Test
    public void shouldReturnNPEWhenMongoClientAsyncIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.getAsync((com.mongodb.async.client.MongoClient) null));
    }

    @AfterAll
    public static void end() {
        MongoDbHelper.stopMongoDb();
    }
}