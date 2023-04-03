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
package org.eclipse.jnosql.databases.couchbase.communication;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.SettingsBuilder;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.Map;

public final class CouchbaseUtil {

    public static final String BUCKET_NAME = "jnosql";

    private static final String FILE_CONFIGURATION = "couchbase.properties";

    private CouchbaseUtil() {
    }

    public static Settings getSettings() {
        Map<String, String> map = ConfigurationReader.from(CouchbaseUtil.FILE_CONFIGURATION);
        SettingsBuilder builder = Settings.builder();
        map.forEach(builder::put);
        return builder.build();
    }
}
