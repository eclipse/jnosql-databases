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
package org.eclipse.jnosql.databases.cassandra.mapping;

import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.column.ColumnEntityConverter;
import org.eclipse.jnosql.mapping.column.ColumnFieldValue;
import org.eclipse.jnosql.mapping.reflection.FieldMapping;
import org.eclipse.jnosql.databases.cassandra.communication.UDT;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static java.util.stream.StreamSupport.stream;

class CassandraUDTType implements ColumnFieldValue {

    private final String type;

    private final Object value;

    private final FieldMapping field;


    CassandraUDTType(String type, Object value, FieldMapping field) {
        this.value = value;
        this.type = type;
        this.field = Objects.requireNonNull(field, "field is required");
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public FieldMapping getField() {
        return field;
    }

    @Override
    public boolean isNotEmpty() {
        return value != null;
    }

    @Override
    public List<Column> toColumn(ColumnEntityConverter converter, Converters converters) {
        if (Iterable.class.isInstance(value)) {
            List<Iterable<Column>> columns = new ArrayList<>();
            stream(Iterable.class.cast(value).spliterator(), false)
                    .forEach(c -> columns.add(converter.toColumn(c).columns()));
            return singletonList(UDT.builder(type).withName(field.name()).addUDTs(columns).build());

        } else {
            return singletonList(UDT.builder(type)
                    .withName(field.name())
                    .addUDT(converter.toColumn(value).columns())
                    .build());
        }
    }

    @Override
    public String toString() {
        return "CassandraUDTType{" +
                "type='" + type + '\'' +
                ", value=" + value +
                ", field=" + field +
                '}';
    }
}
