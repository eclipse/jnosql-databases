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
package org.eclipse.jnosql.communication.hbase.column;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.column.ColumnManager;
import org.eclipse.jnosql.communication.tck.communication.driver.column.ColumnManagerSupplier;

public class HBaseColumnManagerSupplier implements ColumnManagerSupplier {

    private static final String DATABASE = "tck-database";
    public static final String FAMILY = "person";

    @Override
    public ColumnManager get() {
        HBaseColumnConfiguration configuration = new HBaseColumnConfiguration();
        configuration.add(FAMILY);
        final HBaseColumnManagerFactory factory = configuration.apply(Settings.builder().build());
        return factory.apply(DATABASE);
    }

}
