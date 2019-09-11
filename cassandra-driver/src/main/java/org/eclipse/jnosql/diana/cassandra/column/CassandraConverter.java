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

package org.eclipse.jnosql.diana.cassandra.column;


import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.reflect.TypeToken;
import jakarta.nosql.Value;
import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;


final class CassandraConverter {

    private static final CodecRegistry CODE_REGISTRY = CodecRegistry.DEFAULT_INSTANCE;

    private CassandraConverter() {
    }

    public static ColumnEntity toDocumentEntity(Row row) {
        List<Column> columns = new ArrayList<>();
        String columnFamily = "";
        for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
            columnFamily = definition.getTable();
            Object result = CassandraConverter.get(definition, row);
            if (Objects.nonNull(result)) {
                columns.add(getColumn(definition, result));
            }
        }
        return ColumnEntity.of(columnFamily, columns);
    }

    private static Column getColumn(ColumnDefinitions.Definition definition, Object result) {
        switch (definition.getType().getName()) {
            case UDT:
                return Column.class.cast(result);
            case LIST:
            case SET:
                if (isUDTIterable(result)) {
                    return UDT.builder(getUserType(definition, result)).withName(definition.getName())
                            .addUDTs(getColumns(definition, result)).build();

                }
                return Column.of(definition.getName(), Value.of(result));
            default:
                return Column.of(definition.getName(), Value.of(result));
        }
    }

    private static Iterable<Iterable<Column>> getColumns(ColumnDefinitions.Definition definition, Object result) {
        return (Iterable<Iterable<Column>>)
                StreamSupport.stream(Iterable.class.cast(result).spliterator(), false)
                        .map(c -> getUDT(definition.getName(), (UDTValue) c).get())
                        .collect(toList());
    }

    private static String getUserType(ColumnDefinitions.Definition definition, Object result) {
        return StreamSupport.stream(Iterable.class.cast(result).spliterator(), false)
                .limit(1L)
                .map(c -> getUDT(definition.getName(), (UDTValue) c).getUserType())
                .findFirst()
                .get().toString();
    }


    public static Object get(ColumnDefinitions.Definition definition, Row row) {

        String name = definition.getName();
        switch (definition.getType().getName()) {
            case LIST:
                DataType typeList = definition.getType().getTypeArguments().get(0);
                TypeToken<Object> javaTypeList = CODE_REGISTRY.codecFor(typeList).getJavaType();
                return row.getList(name, javaTypeList);
            case SET:
                DataType typeSet = definition.getType().getTypeArguments().get(0);
                TypeToken<Object> javaTypeSet = CODE_REGISTRY.codecFor(typeSet).getJavaType();
                return row.getSet(name, javaTypeSet);
            case MAP:
                DataType typeKey = definition.getType().getTypeArguments().get(0);
                DataType typeValue = definition.getType().getTypeArguments().get(1);
                TypeToken<Object> javaTypeKey = CODE_REGISTRY.codecFor(typeKey).getJavaType();
                TypeToken<Object> javaTypeValue = CODE_REGISTRY.codecFor(typeValue).getJavaType();
                return row.getMap(name, javaTypeKey, javaTypeValue);
            case UDT:
                UDTValue udtValue = row.getUDTValue(name);
                return getUDT(name, udtValue);
            default:
                TypeCodec<Object> objectTypeCodec = CODE_REGISTRY.codecFor(definition.getType());
                return row.get(name, objectTypeCodec);
        }


    }

    private static UDT getUDT(String name, UDTValue udtValue) {
        List<Column> columns = new ArrayList<>();
        UserType type = udtValue.getType();
        for (String fieldName : type.getFieldNames()) {
            DataType fieldType = type.getFieldType(fieldName);
            Object elementValue = udtValue.get(fieldName, CODE_REGISTRY.codecFor(fieldType));
            if (elementValue != null) {
                columns.add(Column.of(fieldName, elementValue));
            }
        }
        return UDT.builder(type.getTypeName()).withName(name).addUDT(columns).build();
    }

    private static boolean isUDTIterable(Object result) {
        final Iterable<?> iterable = Iterable.class.cast(result);
        if (!iterable.iterator().hasNext()) {
            return false;
        }
        return StreamSupport.stream(iterable.spliterator(), false)
                .allMatch(UDTValue.class::isInstance);
    }


}
