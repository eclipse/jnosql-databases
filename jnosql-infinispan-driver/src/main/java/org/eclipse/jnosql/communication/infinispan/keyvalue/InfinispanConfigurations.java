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
 *   The Infinispan Team
 */
package org.eclipse.jnosql.communication.infinispan.keyvalue;

import java.util.function.Supplier;

/**
 * An enumeration to show the available options to connect to the Infinispan database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see jakarta.nosql.Settings
 */
public enum InfinispanConfigurations implements Supplier<String> {

    /**
     * Database's host. It is a prefix to enumerate hosts. E.g.: infinispan.host.1=HOST
     */
    HOST("infinispan.host"),
    /**
     * The Infinispan configuration path. E.g.: infinispan.config=infinispan.xml
     */
    CONFIG("infinispan.config");

    private final String configuration;

    InfinispanConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
