/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
        throw new UnsupportedOperationException("This method is not supported, use getColumns instead");
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public Object get() {
        return columns;
    }

    @Override
    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
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
                Objects.equals(columns, udt.getColumns());
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
