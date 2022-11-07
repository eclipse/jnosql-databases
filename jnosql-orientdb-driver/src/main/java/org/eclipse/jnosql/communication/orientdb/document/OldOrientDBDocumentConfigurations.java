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
package org.eclipse.jnosql.communication.orientdb.document;

import java.util.function.Supplier;

/**
 * Use {@link OrientDBDocumentConfigurations}
 */
@Deprecated
public enum  OldOrientDBDocumentConfigurations implements Supplier<String> {

    HOST("orientdb-server-host"),
    USER("orientdb-server-user"),
    PASSWORD("orientdb-server-password"),
    STORAGE_TYPE("orientdb-server-storageType");

    private final String configuration;

    OldOrientDBDocumentConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
