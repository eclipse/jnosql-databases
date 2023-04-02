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
package org.eclipse.jnosql.cassandra.communication.column;

import org.eclipse.jnosql.communication.TypeSupplier;
import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.column.Column;

import java.util.Objects;

/**
 * On Cassandra, there is the option to a UDT be part of a list. This implementation holds this option.
 */
class IterableUDT implements UDT {

    private final String name;

    private final String userType;

    private final Iterable<Iterable<Column>> columns;

    IterableUDT(String name, String userType, Iterable<Iterable<Column>> columns) {
        this.name = name;
        this.userType = userType;
        this.columns = columns;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Value value() {
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
        return Objects.equals(name, udt.name()) &&
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
