/*
 *  Copyright (c) 2020 Otávio Santana and others
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

import jakarta.nosql.ServiceLoaderProvider;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnFamilyManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnFamilyManagerTest {

    @ParameterizedTest
    @ColumnSource("column_insert.properties")
    public void shouldInsert(ColumnArgument argument) {
        Assumptions.assumeFalse(argument.isEmpty(), "The you put the  file in the resources to activate this test");
        ColumnFamilyManager manager = getManager();
        Optional<ColumnEntity> entity = argument.getQuery().stream().flatMap(manager::query)
                .findFirst();


    }

    private ColumnFamilyManager getManager() {
        final ColumnFamilyManagerSupplier supplier = ServiceLoaderProvider.get(ColumnFamilyManagerSupplier.class);
        return supplier.get();
    }

}
