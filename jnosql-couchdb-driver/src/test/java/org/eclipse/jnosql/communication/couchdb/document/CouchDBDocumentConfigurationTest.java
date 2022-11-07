/*
 *
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
 *
 */
package org.eclipse.jnosql.communication.couchdb.document;

import jakarta.nosql.document.DocumentCollectionManagerFactory;
import jakarta.nosql.document.DocumentConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class CouchDBDocumentConfigurationTest {

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByMap() {
        CouchDBDocumentConfiguration configuration = new CouchDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByFile() {
        CouchDBDocumentConfiguration configuration = new CouchDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldReturnFromConfiguration() {
        CouchDBDocumentConfiguration configuration = DocumentConfiguration.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof CouchDBDocumentConfiguration);
    }

    @Test
    public void shouldReturnFromConfigurationQuery() {
        CouchDBDocumentConfiguration configuration = DocumentConfiguration
                .getConfiguration(CouchDBDocumentConfiguration.class);
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof CouchDBDocumentConfiguration);
    }

}