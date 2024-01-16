/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.KeyValueConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * The Oracle implementation of {@link KeyValueConfiguration} that returns
 * {@link OracleBucketManagerFactory}.
 *
 */
public class OracleKeyValueConfiguration implements KeyValueConfiguration {
    @Override
    public BucketManagerFactory apply(Settings settings) {
        requireNonNull(settings, "settings is required");

        var nosql =  NoSQLHandleConfigConfiguration.INSTANCE.apply(settings);
        return new OracleBucketManagerFactory(nosql);
    }
}
