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

package org.eclipse.jnosql.diana.mongodb.document;

import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentCollectionManagerAsyncFactory;
import jakarta.nosql.document.DocumentCollectionManagerFactory;
import jakarta.nosql.document.DocumentConfiguration;
import jakarta.nosql.document.DocumentConfigurationAsync;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MongoDBDocumentConfigurationAsyncTest {

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByMap() {
        Map<String, String> map = new HashMap<>();
        map.put("mongodb-server-host-1", "172.17.0.2:27017");
        MongoDBDocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get(map);
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateSettings() {
        Settings settings = Settings.builder().put("mongodb-server-host-1", "172.17.0.2:27017").build();
        MongoDBDocumentConfigurationAsync configuration = new MongoDBDocumentConfigurationAsync();
        DocumentCollectionManagerAsyncFactory managerFactory = configuration.get(settings);
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByFile() {
        DocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldReturnErrorWhendSettingsIsNull() {
        DocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        assertThrows(NullPointerException.class, () -> configuration.get(null));
    }

    @Test
    public void shouldReturnErrorWhenMapSettingsIsNull() {
        MongoDBDocumentConfiguration configuration = new MongoDBDocumentConfiguration();
        assertThrows(NullPointerException.class, () -> configuration.get((Map) null));
    }

    @Test
    public void shouldReturnFromConfiguration() {
        DocumentConfigurationAsync configuration = DocumentConfigurationAsync.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof DocumentConfigurationAsync);
    }

    @Test
    public void shouldReturnFromConfigurationQuery() {
        MongoDBDocumentConfigurationAsync configuration = DocumentConfigurationAsync
                .getConfiguration(MongoDBDocumentConfigurationAsync.class);
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof MongoDBDocumentConfigurationAsync);
    }

}
