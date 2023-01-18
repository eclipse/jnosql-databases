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
package org.eclipse.jnosql.communication.couchbase.document;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.eclipse.jnosql.communication.couchbase.DatabaseContainer;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CouchbaseDocumentConfigurationTest {

    @Test
    public void shouldCreateDocumentManagerFactoryByMap() {

        CouchbaseDocumentConfiguration configuration = DatabaseContainer.INSTANCE.getDocumentConfiguration();

        DocumentManagerFactory managerFactory = configuration.apply(CouchbaseUtil.getSettings());
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateDocumentManagerFactoryByFile() {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        DocumentManagerFactory managerFactory = configuration.apply(CouchbaseUtil.getSettings());
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldGetConfiguration() {
        DocumentConfiguration configuration = DocumentConfiguration.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof CouchbaseDocumentConfiguration);
    }

    @Test
    public void shouldGetConfigurationFromQuery() {
        CouchbaseDocumentConfiguration configuration = DocumentConfiguration
                .getConfiguration(CouchbaseDocumentConfiguration.class);
        Assertions.assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof CouchbaseDocumentConfiguration);
    }

}