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

import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentCollectionManagerFactory;
import org.jnosql.diana.api.document.DocumentConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RavenDBDocumentConfigurationTest {

    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("ravendb-server-host-1", "172.17.0.2:8080");
        RavenDBDocumentConfiguration configuration = new RavenDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get(Settings.of(map));
        assertNotNull(managerFactory);
    }


    @Test
    public void shouldCreateDocumentCollectionManagerFactoryByFile() {
        DocumentConfiguration configuration = new RavenDBDocumentConfiguration();
        DocumentCollectionManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldReturnErrorWhendSettingsIsNull() {
        DocumentConfiguration configuration = new RavenDBDocumentConfiguration();
        assertThrows(NullPointerException.class, () -> configuration.get(null));
    }


}
