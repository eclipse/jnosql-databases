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


import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeToken;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import jakarta.nosql.Value;
import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnEntity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.reducing;
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
        switch (definition.getType().getProtocolCode()) {
            case ProtocolConstants.DataType.UDT:
                return Column.class.cast(result);
            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.SET:
                return Column.of(definition.getName().asInternal(), Value.of(result));
            default:
                return Column.of(definition.getName().asInternal(), Value.of(result));
        }
    }

    static Object get(ColumnDefinition definition, Row row) {

        String name = definition.getName().asInternal();
        final DataType type = definition.getType();
        final TypeCodec<Object> codec = row.codecRegistry().codecFor(type);
        return row.get(name, codec);
    }

}
