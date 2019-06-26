/*
 *  Copyright (c) 2017 Otávio Santana and others
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

package org.jnosql.diana.solr.document;

import com.mongodb.ServerAddress;

class HostPortConfiguration {


    private final String host;

    private final int port;

    HostPortConfiguration(String value) {
        String[] values = value.split(":");
        if (values.length == 2) {
            host = values[0];
            port = Integer.valueOf(values[1]);
        } else {
            host = values[0];
            port = MongoDBDocumentConfiguration.DEFAULT_PORT;
        }
    }

    ServerAddress toServerAddress() {
        return new ServerAddress(host, port);
    }
}
