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
package org.jnosql.diana.cassandra.column;


import org.jnosql.diana.api.TypeSupplier;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.column.Column;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The default implementation of {@link UDT}
 */
class DefaultUDT implements UDT {

    private final String name;

    private final String userType;

    private final List<Column> columns;

    DefaultUDT(String name, String userType, List<Column> columns) {
        this.name = name;
        this.userType = userType;
        this.columns = columns;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Value getValue() {
        return Value.of(columns);
    }

    @Override
    public <T> T get(Class<T> clazz) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(clazz, "clazz is required");
        return Value.of(columns).get(clazz);
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(typeSupplier, "typeSupplier is required");
        return Value.of(columns).get(typeSupplier);

    }

    @Override
    public Object get() {
        return columns;
    }

    @Override
    public String getUserType() {
        return userType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UDT)) {
            return false;
        }
        UDT udt = (UDT) o;
        return Objects.equals(name, udt.getName()) &&
                Objects.equals(userType, udt.getUserType()) &&
                Objects.equals(columns, udt.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, userType, columns);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UDT{");
        sb.append("name='").append(name).append('\'');
        sb.append(", userType='").append(userType).append('\'');
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
