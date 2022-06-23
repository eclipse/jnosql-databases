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

package org.eclipse.jnosql.communication.arangodb.document;

import jakarta.nosql.document.DocumentCollectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ArangoDBDocumentCollectionManagerFactoryTest {

    private ArangoDBDocumentCollectionManagerFactory managerFactory;

    @BeforeEach
    public void setUp() {
        managerFactory = ArangoDBDocumentCollectionManagerFactorySupplier.INSTANCE.get();
    }

    @Test
    public void shouldCreateEntityManager() {
        DocumentCollectionManager database = managerFactory.get("database");
        assertNotNull(database);
    }

}