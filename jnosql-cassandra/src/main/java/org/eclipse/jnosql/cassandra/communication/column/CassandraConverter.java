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


import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.communication.column.ColumnEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

final class CassandraConverter {

    private CassandraConverter() {
    }

    public static ColumnEntity toDocumentEntity(Row row) {
        List<Column> columns = new ArrayList<>();
        String columnFamily = "";
        for (ColumnDefinition definition : row.getColumnDefinitions()) {
            columnFamily = definition.getTable().asInternal();
            Object result = CassandraConverter.get(definition, row);
            if (Objects.nonNull(result)) {
                columns.add(getColumn(definition, result));
            }
        }
        return ColumnEntity.of(columnFamily, columns);
    }


    private static Column getColumn(ColumnDefinition definition, Object result) {

        final DataType type = definition.getType();
        switch (type.getProtocolCode()) {
            case ProtocolConstants.DataType.UDT:
                return Column.class.cast(result);
            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.SET:
                if (isUDTIterable(result)) {
                    return UDT.builder(getUserType(result)).withName(definition.getName().asInternal())
                            .addUDTs(getColumns(definition, result)).build();
                }
                return Column.of(definition.getName().asInternal(), Value.of(result));
            default:
                return Column.of(definition.getName().asInternal(), Value.of(result));
        }
    }

    static Object get(ColumnDefinition definition, Row row) {

        String name = definition.getName().asInternal();
        final DataType type = definition.getType();
        if (type instanceof UserDefinedType) {
            return getUDT(definition, row.getUdtValue(name));
        }
        final TypeCodec<Object> codec = row.codecRegistry().codecFor(type);
        return row.get(name, codec);
    }

    private static UDT getUDT(ColumnDefinition definition, UdtValue udtValue) {
        String name = definition.getName().asInternal();
        final UserDefinedType type = udtValue.getType();
        List<Column> columns = new ArrayList<>();
        List<String> names = type.getFieldNames().stream().map(CqlIdentifier::asInternal).collect(toList());
        for (CqlIdentifier fieldName : type.getFieldNames()) {
            final int index = names.indexOf(fieldName.asInternal());
            DataType fieldType = type.getFieldTypes().get(index);
            Object elementValue = udtValue.get(fieldName, CodecRegistry.DEFAULT.codecFor(fieldType));
            if (elementValue != null) {
                columns.add(Column.of(fieldName.asInternal(), elementValue));
            }
        }
        return UDT.builder(type.getName().asInternal()).withName(name).addUDT(columns).build();
    }

    private static String getUserType(Object result) {
        return StreamSupport.stream(Iterable.class.cast(result).spliterator(), false)
                .limit(1L)
                .map(c -> UdtValue.class.cast(c).getType().getName().asInternal())
                .findFirst()
                .get().toString();
    }

    private static Iterable<Iterable<Column>> getColumns(ColumnDefinition definition, Object result) {

        List<Iterable<Column>> columns = new ArrayList<>();
        for (Object value : Iterable.class.cast(result)) {
            final UdtValue udtValue = UdtValue.class.cast(value);
            final UDT udt = getUDT(definition, udtValue);
            columns.add((Iterable<Column>) udt.get());
        }

        return columns;
    }

    private static boolean isUDTIterable(Object result) {
        final Iterable<?> iterable = Iterable.class.cast(result);
        if (!iterable.iterator().hasNext()) {
            return false;
        }
        return StreamSupport.stream(iterable.spliterator(), false)
                .allMatch(UdtValue.class::isInstance);
    }


}
