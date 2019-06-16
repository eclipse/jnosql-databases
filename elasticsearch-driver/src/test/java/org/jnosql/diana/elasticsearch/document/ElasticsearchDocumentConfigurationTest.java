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
package org.jnosql.diana.elasticsearch.document;

import jakarta.nosql.document.DocumentCollectionManagerFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ElasticsearchDocumentConfigurationTest {

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByMap() {
        DocumentCollectionManagerFactory managerFactory = ElasticsearchDocumentCollectionManagerFactorySupplier.INSTACE.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByFile() {
        DocumentCollectionManagerFactory managerFactory = ElasticsearchDocumentCollectionManagerFactorySupplier.INSTACE.get();
        assertNotNull(managerFactory);
    }
}