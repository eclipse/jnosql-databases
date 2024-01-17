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

import java.util.function.Supplier;

/**
 * An enumeration representing the available configuration options for connecting to the Oracle NoSQL database.
 * Each option implements the {@link java.util.function.Supplier} interface, which returns the corresponding property name.
 * These properties can be overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see org.eclipse.jnosql.communication.Settings
 */
public enum OracleConfigurations implements Supplier<String> {

    /**
     * Specifies the hostname or IP address of the Oracle NoSQL database server.
     * Default value: <b>http://localhost:8080</b>
     */
    HOST("jnosql.oracle.host"),

    /**
     * Specifies the username used to authenticate with the Oracle NoSQL database.
     */
    USER("jnosql.oracle.user"),

    /**
     * Specifies the password used to authenticate with the Oracle NoSQL database.
     */
    PASSWORD("jnosql.oracle.password"),

    /**
     * Specifies the desired throughput of read operations when creating a table using Eclipse JNoSQL.
     * A read unit represents 1 eventually consistent read per second for data up to 1 KB in size.
     * Double the read units are consumed for absolutely consistent reads.
     * Default value: <b>25</b>
     */
    TABLE_READ_LIMITS("jnosql.oracle.table.read.limit"),

    /**
     * Specifies the desired throughput of write operations when creating a table using Eclipse JNoSQL.
     * A write unit represents 1 write per second of data up to 1 KB in size.
     * Default value: <b>25</b>
     */
    TABLE_WRITE_LIMITS("jnosql.oracle.table.write.limit"),

    /**
     * Specifies the maximum storage, in gigabytes, to be consumed by a table created using Eclipse JNoSQL.
     * Default value: <b>25</b>
     */
    TABLE_STORAGE_GB("jnosql.oracle.table.storage.gb"),

    /**
     * Specifies the total amount of time to wait, in milliseconds, when creating a table.
     * This value must be non-zero and greater than the delayMillis.
     */
    TABLE_WAIT_MILLIS("jnosql.oracle.table.wait.millis"),

    /**
     * Specifies the amount of time to wait, in milliseconds, between polling attempts when creating a table.
     * If set to 0, it defaults to <b>500</b> milliseconds.
     */
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