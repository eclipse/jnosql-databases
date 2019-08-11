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
package org.jnosql.diana.couchbase.configuration;

import org.jnosql.diana.couchbase.kv.CouchbaseKeyValueConfiguration;

public class CouchbaseKeyValueTcConfiguration extends CouchbaseKeyValueConfiguration {

    private static CouchbaseKeyValueTcConfiguration tcConfiguration;

    private CouchbaseKeyValueTcConfiguration() {
        CouchbaseContainer couchbase = CouchbaseContainer.start(user, password);
        nodes.clear();
        add(couchbase.getContainer().getContainerIpAddress());
    }

    public static CouchbaseKeyValueTcConfiguration getTcConfiguration() {
        if (tcConfiguration == null) {
            tcConfiguration = new CouchbaseKeyValueTcConfiguration();
        }
        return tcConfiguration;
    }
}
