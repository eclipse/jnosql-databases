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
package org.eclipse.jnosql.databases.oracle.communication;

import java.util.function.Supplier;

/**
 * An enumeration to show the available options to connect to the Oracle NoSQL database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see org.eclipse.jnosql.communication.Settings
 */
public enum OracleConfigurations implements Supplier<String> {

    HOST("jnosql.oracle.host"),
    USER("jnosql.oracle.user"),
    PASSWORD("jnosql.oracle.password"),
    TABLE_READ_LIMITS("jnosql.oracle.table.read.limit"),
    TABLE_WRITE_LIMITS("jnosql.oracle.table.write.limit"),
    TABLE_STORAGE_GB("jnosql.oracle.table.storage.gb"),
    TABLE_WAIT_MILLIS("jnosql.oracle.table.wait.millis"),
    TABLE_DELAY_MILLIS("jnosql.oracle.table.delay.millis");


    private final String configuration;

    OracleConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
