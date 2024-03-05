/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import jakarta.json.bind.Jsonb;
import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import org.eclipse.jnosql.communication.semistructured.DatabaseManagerFactory;

import java.util.Objects;


/**
 * The Oracle implementation to {@link DatabaseManagerFactory}
 */
public final class OracleDocumentManagerFactory implements DatabaseManagerFactory {

    private static final Jsonb JSON = JsonbSupplier.getInstance().get();
    private final NoSQLHandleConfiguration configuration;

    OracleDocumentManagerFactory(NoSQLHandleConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void close() {
    }

    @Override
    public OracleNoSQLDocumentManager apply(String table) {
        Objects.requireNonNull(table, "table is required");
        var tableCreation = this.configuration.tableCreationConfiguration();
        tableCreation.createTable(table, configuration.serviceHandle());
        return new DefaultOracleNoSQLDocumentManager(table, configuration.serviceHandle(), JSON);
    }
}
