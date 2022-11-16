/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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

package org.eclipse.jnosql.communication.mongodb.document;

import com.mongodb.client.MongoClient;
import jakarta.nosql.Settings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MongoDBDocumentManagerFactoryTest {

    private static MongoDBDocumentConfiguration configuration;

    @BeforeAll
    public static void setUp() throws IOException {
        configuration = new MongoDBDocumentConfiguration();
    }

    @Test
    public void shouldCreateEntityManager() {
        MongoDBDocumentManagerFactory mongoDBFactory = configuration.apply(Settings.builder().build());
        assertNotNull(mongoDBFactory.apply("database"));
    }

    @Test
    public void shouldReturnNPEWhenSettingsIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.apply((Settings) null));
    }

    @Test
    public void shouldReturnNPEWhenMapSettingsIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.get((Map<String, String>) null));
    }

    @Test
    public void shouldReturnNPEWhenMongoClientIsNull() {
        assertThrows(NullPointerException.class, () -> configuration.get((MongoClient) null));
    }

}