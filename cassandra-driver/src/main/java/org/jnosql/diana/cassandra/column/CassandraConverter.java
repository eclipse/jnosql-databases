/*
 * Copyright 2017 Otavio Santana and others
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.jnosql.diana.cassandra.column;


import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.reflect.TypeToken;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.document.Document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

final class CassandraConverter {

    private static CodecRegistry CODE_REGISTRY = CodecRegistry.DEFAULT_INSTANCE;

    private CassandraConverter() {
    }

    public static ColumnEntity toDocumentEntity(Row row) {
        List<Column> columns = new ArrayList<>();
        String columnFamily = "";
        for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
            DataType type = definition.getType();
            columnFamily = definition.getTable();
            Value value = Value.of(CassandraConverter.get(definition, row));
            Column column = Column.of(definition.getName(), value);
            columns.add(column);
        }
        return ColumnEntity.of(columnFamily, columns);
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
                return row.getList(name, javaTypeSet);
            case MAP:
                DataType typeKey = definition.getType().getTypeArguments().get(0);
                DataType typeValue = definition.getType().getTypeArguments().get(1);
                TypeToken<Object> javaTypeKey = CODE_REGISTRY.codecFor(typeKey).getJavaType();
                TypeToken<Object> javaTypeValue = CODE_REGISTRY.codecFor(typeValue).getJavaType();
                return row.getMap(name, javaTypeKey, javaTypeValue);
            case UDT:
                UDTValue udtValue = row.getUDTValue(name);
                UserType type = udtValue.getType();
                List<Column> columns = new ArrayList<>();
                for (String fieldName : type.getFieldNames()) {
                    DataType fieldType = type.getFieldType(fieldName);
                    Object elementValue = udtValue.get(fieldName, CODE_REGISTRY.codecFor(fieldType));
                    if (elementValue != null) {
                        columns.add(Column.of(fieldName, elementValue));
                    }
                }
                return new UDT(name, type.getTypeName(), columns);
        }

        TypeCodec<Object> objectTypeCodec = CODE_REGISTRY.codecFor(definition.getType());
        return row.get(name, objectTypeCodec);

    }


}
