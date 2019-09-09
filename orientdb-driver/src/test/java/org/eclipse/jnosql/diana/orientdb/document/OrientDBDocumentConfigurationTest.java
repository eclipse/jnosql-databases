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

package org.eclipse.jnosql.diana.orientdb.document;

import jakarta.nosql.document.DocumentCollectionManagerFactory;
import jakarta.nosql.document.DocumentConfiguration;
import jakarta.nosql.document.DocumentConfigurationAsync;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OrientDBDocumentConfigurationTest {

    @Test
    public void shouldCreateDocumentCollectionManagerFactory() {
        OrientDBDocumentConfiguration configuration = new OrientDBDocumentConfiguration();
        configuration.setHost("172.17.0.2");
        configuration.setUser("root");
        configuration.setPassword("rootpwd");
        DocumentCollectionManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateByConfigurationFile() {
        OrientDBDocumentConfiguration configuration = new OrientDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldThrowExceptionWhenSettingsIsNull() {
        assertThrows(NullPointerException.class, () -> new OrientDBDocumentConfiguration().get(null));
    }

    @Test
    public void shouldReturnFromConfiguration() {
        DocumentConfiguration configuration = DocumentConfiguration.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof DocumentConfiguration);
    }

    @Test
    public void shouldReturnFromConfigurationQuery() {
        OrientDBDocumentConfiguration configuration = DocumentConfiguration
                .getConfiguration(OrientDBDocumentConfiguration.class);
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof OrientDBDocumentConfiguration);
    }

    @Test
    public void shouldReturnFromConfigurationAsync() {
        DocumentConfigurationAsync configuration = DocumentConfigurationAsync.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof DocumentConfiguration);
    }

    @Test
    public void shouldReturnFromConfigurationAsyncQuery() {
        DocumentConfigurationAsync configuration = DocumentConfigurationAsync
                .getConfiguration(OrientDBDocumentConfiguration.class);
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof OrientDBDocumentConfiguration);
    }
}
