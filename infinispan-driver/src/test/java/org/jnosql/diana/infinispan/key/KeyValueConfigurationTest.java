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
 *   The Infinispan Team
 */
package org.jnosql.diana.infinispan.key;

import java.util.HashMap;
import java.util.Map;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KeyValueConfigurationTest {

    private InfinispanKeyValueConfiguration configuration;

    @BeforeEach
    public void setUp() {
        configuration = new InfinispanKeyValueConfiguration();
    }

    @Test
    public void shouldCreateKeyValueFactory() {
        Map<String, String> map = new HashMap<>();
        BucketManagerFactory managerFactory = configuration.get(map);
        assertNotNull(managerFactory);
    }

    @Test
    public void shouldCreateKeyValueFactoryFromFile() {
        BucketManagerFactory managerFactory = configuration.get();
        assertNotNull(managerFactory);
    }

}
