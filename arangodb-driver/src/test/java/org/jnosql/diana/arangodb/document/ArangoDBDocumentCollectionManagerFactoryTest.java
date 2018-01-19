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

package org.jnosql.diana.arangodb.document;

import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ArangoDBDocumentCollectionManagerFactoryTest {

    private ArangoDBDocumentCollectionManagerFactory managerFactory;

    @BeforeEach
    public void setUp() {
        managerFactory = DocumentConfigurationUtils.getConfiguration();
    }

    @Test
    public void shouldCreateEntityManager() {
        DocumentCollectionManager database = managerFactory.get("database");
        assertNotNull(database);
    }

    @Test
    public void shouldCreateEntityManagerAsync() {
        DocumentCollectionManagerAsync database = managerFactory.getAsync("database");
        assertNotNull(database);
    }
}