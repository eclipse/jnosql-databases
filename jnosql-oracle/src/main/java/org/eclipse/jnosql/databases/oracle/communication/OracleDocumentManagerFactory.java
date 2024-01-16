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

import oracle.nosql.driver.NoSQLHandle;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;

import java.util.Objects;


/**
 * The Oracle implementation to {@link DocumentManagerFactory}
 */
class OracleDocumentManagerFactory implements DocumentManagerFactory {
    private final NoSQLHandle serviceHandle;

    public OracleDocumentManagerFactory(NoSQLHandle serviceHandle) {
        this.serviceHandle = serviceHandle;
    }

    @Override
    public void close() {
        this.serviceHandle.close();
    }

    @Override
    public DocumentManager apply(String table) {
        Objects.requireNonNull(table, "table is required");
        return null;
    }
}
