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
 * An enumeration to show the available options to connect to the OrientDB database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see jakarta.nosql.Settings
 */
public enum OrientDBDocumentConfigurations implements Supplier<String> {

    /**
     * The database host
     */
    HOST("orientdb.host"),
    /**
     * The user's credential.
     */
    USER("orientdb.-user"),
    /**
     * The password's credential
     */
    PASSWORD("orientdb.password"),
    /**
     * The storage type {@link com.orientechnologies.orient.core.db.ODatabaseType}
     */
    STORAGE_TYPE("orientdb.storage.type");

    private final String configuration;

    OrientDBDocumentConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
