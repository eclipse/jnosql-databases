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

package org.jnosql.diana.mongodb.document;

import org.jnosql.diana.api.Settings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;


public class MongoDBDocumentCollectionManagerFactoryTest {

    private static MongoDBDocumentConfiguration configuration;

    @BeforeClass
    public static void setUp() throws IOException {
        configuration = new MongoDBDocumentConfiguration();
        MongoDbHelper.startMongoDb();
    }

    @Test
    public void shouldCreateEntityManager() {
        MongoDBDocumentCollectionManagerFactory mongoDBFactory = configuration.get();
        assertNotNull(mongoDBFactory.get("database"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnNPEWhenSettingsIsNull() {
        configuration.get((Settings) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnNPEWhenMapSettingsIsNull() {
        configuration.get((Map<String, String>) null);
    }


    @Test
    public void shouldCreateEntityManagerAsync() {
        MongoDBDocumentCollectionManagerAsyncFactory mongoDBFactory = configuration.getAsync();
        assertNotNull(mongoDBFactory.getAsync("database"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnNPEWhenSettingOnAsyncsIsNull() {
        configuration.getAsync((Settings) null);
    }

    @AfterClass
    public static void end() {
        MongoDbHelper.stopMongoDb();
    }
}