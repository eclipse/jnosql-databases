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


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import jakarta.nosql.Value;
import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnEntity;
import org.eclipse.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class QueryUtils {


    private QueryUtils() {
    }


    public static RegularInsert insert(ColumnEntity entity, String keyspace, CqlSession session) {

        Map<String, Term> values = new HashMap<>();
        InsertInto insert = QueryBuilder.insertInto(keyspace, entity.getName());
        entity.getColumns().stream()
                .forEach(c -> {
                    if (UDT.class.isInstance(c)) {
                        insertUDT(UDT.class.cast(c), keyspace, session, insert);
                    } else {
                        insertSingleField(c, values);
                    }
                });

        return insert.values(values);
    }

    private static void insertUDT(UDT udt, String keyspace, CqlSession session, InsertInto insert) {

        UserDefinedType userType =
                session.getMetadata()
                        .getKeyspace(keyspace)
                        .flatMap(ks -> ks.getUserDefinedType(udt.getUserType()))
                        .orElseThrow(() -> new IllegalArgumentException("Missing UDT definition"));

        Iterable elements = Iterable.class.cast(udt.get());
        Object udtValue = getUdtValue(userType, elements);
        insert.value(getName(udt), QueryBuilder.literal(udtValue));
    }

    private static Object getUdtValue(UserDefinedType userType, Iterable elements) {

        List<Object> udtValues = new ArrayList<>();
        UdtValue udtValue = userType.newValue();
        for (Object object : elements) {
            if (Column.class.isInstance(object)) {
                Column column = Column.class.cast(object);
                Object convert = ValueUtil.convert(column.getValue());

                //DataType fieldType = userType.getFieldType(column.getName());
                DataType fieldType = null;
                TypeCodec<Object> objectTypeCodec = CodecRegistry.DEFAULT.codecFor(fieldType);
                udtValue.set(getName(column), convert, objectTypeCodec);

            } else if (Iterable.class.isInstance(object)) {
                udtValues.add(getUdtValue(userType, Iterable.class.cast(Iterable.class.cast(object))));
            }
        }
        if (udtValues.isEmpty()) {
            return udtValue;
        }
        return udtValues;

    }

    private static void insertSingleField(Column column, Map<String, Term> values) {
        Object value = column.get();
        try {
            CodecRegistry.DEFAULT.codecFor(value);
            values.put(getName(column), QueryBuilder.literal(value));
        } catch (CodecNotFoundException exp) {
            values.put(getName(column), QueryBuilder.literal(ValueUtil.convert(column.getValue())));
        }
    }


    public static String count(String columnFamily, String keyspace) {
        return String.format("select count(*) from %s.%s", keyspace, columnFamily);
    }

    private static String getName(Column column) {

        String name = column.getName();
        if (name.charAt(0) == '_') {
            return "\"" + name + "\"";
        }
        return name;
    }

    private static Object[] getIinValue(Value value) {
        return ValueUtil.convertToList(value).toArray();
    }


}
