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

package jakarta.nosql.tck.communication.driver.column;


import jakarta.nosql.Settings;
import jakarta.nosql.column.ColumnConfiguration;
import jakarta.nosql.column.ColumnFamilyManagerFactory;
import org.eclipse.jnosql.diana.cassandra.column.CassandraConfiguration;
import org.eclipse.jnosql.diana.cassandra.column.ManagerFactorySupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ColumnConfigurationTest {

    @Test
    public void shouldCreateInstance() {
        final ColumnConfiguration configuration = ColumnConfiguration.getConfiguration();
        Assertions.assertNotNull(configuration);
    }

    @Test
    public void shouldReturnErrorWhenTheParameterIsNull() {
        final ColumnConfiguration configuration = ColumnConfiguration.getConfiguration();
        Assertions.assertThrows(NullPointerException.class, () -> configuration.get(null));
    }


}
