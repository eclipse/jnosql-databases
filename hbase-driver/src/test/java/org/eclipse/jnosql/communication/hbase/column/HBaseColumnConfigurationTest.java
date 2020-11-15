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

package org.eclipse.jnosql.communication.hbase.column;

import jakarta.nosql.column.ColumnConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HBaseColumnConfigurationTest {


    @Test
    public void shouldCreatesColumnFamilyManagerFactory() {
        ColumnConfiguration configuration = new HBaseColumnConfiguration();
        assertNotNull(configuration.get());
    }

    @Test
    public void shouldCreatesColumnFamilyManagerFactoryFromConfiguration() {
        ColumnConfiguration configuration = new HBaseColumnConfiguration();
        assertNotNull(configuration.get());
    }

    @Test
    public void shouldReturnErrorCreatesColumnFamilyManagerFactory() {
        assertThrows(NullPointerException.class, () -> new HBaseColumnConfiguration(null));
    }
}