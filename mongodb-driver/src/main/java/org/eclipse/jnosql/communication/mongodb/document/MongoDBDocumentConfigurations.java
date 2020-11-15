/*
 *  Copyright (c) 2019 Otávio Santana and others
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

package org.eclipse.jnosql.communication.mongodb.document;

import java.util.function.Supplier;

public enum MongoDBDocumentConfigurations implements Supplier<String> {
    HOST("mongodb.host"),
    USER("mongodb.user"),
    URL("mongodb.url"),
    PASSWORD("mongodb.password"),
    AUTHENTICATION_SOURCE("mongodb.authentication.source"),
    AUTHENTICATION_MECHANISM("mongodb.authentication.mechanism");

    private final String configuration;

    MongoDBDocumentConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
