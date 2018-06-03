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


import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnCondition;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnQuery;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static org.jnosql.diana.api.Sort.SortType.ASC;

final class QueryUtils {


    private static final Function<Sort, Ordering> SORT_ORDERING_FUNCTION = sort -> {
        if (ASC.equals(sort.getType())) {
            return asc(sort.getName());
        } else {
            return desc(sort.getName());
        }
    };

    private QueryUtils() {
    }


    public static Insert insert(ColumnEntity entity, String keyspace, Session session) {
        Insert insert = insertInto(keyspace, entity.getName());


        entity.getColumns().stream()
                .forEach(c -> {
                    if (UDT.class.isInstance(c)) {
                        insertUDT(UDT.class.cast(c), keyspace, session, insert);
                    } else {
                        insertSingleField(c, insert);
                    }
                });
        return insert;
    }

    private static void insertUDT(UDT udt, String keyspace, Session session, Insert insert) {
        UserType userType = session.getCluster().getMetadata().getKeyspace(keyspace).getUserType(udt.getUserType());

        Iterable elements = Iterable.class.cast(udt.get());
        Object udtValue = getUdtValue(userType, elements);
        insert.value(getName(udt), udtValue);
    }

    private static Object getUdtValue(UserType userType, Iterable elements) {

        List<Object> udtValues = new ArrayList<>();
        UDTValue udtValue = userType.newValue();
        for (Object object : elements) {
            if (Column.class.isInstance(object)) {
                Column column = Column.class.cast(object);
                Object convert = ValueUtil.convert(column.getValue());
                TypeCodec<Object> objectTypeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(convert);
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

    private static void insertSingleField(Column column, Insert insert) {
        Object value = column.get();
        try {
            CodecRegistry.DEFAULT_INSTANCE.codecFor(value);
            insert.value(getName(column), value);
        } catch (CodecNotFoundException exp) {
            insert.value(getName(column), ValueUtil.convert(column.getValue()));
        }


    }


    public static BuiltStatement select(ColumnQuery query, String keySpace) {
        String columnFamily = query.getColumnFamily();

        if (Objects.isNull(query.getCondition())) {
            return QueryBuilder.select().all().from(keySpace, columnFamily);
        }
        Select.Where where = QueryBuilder.select().all().from(keySpace, columnFamily).where();
        if (query.getLimit() > 0) {
            where.limit((int) query.getLimit());
        }
        if (!query.getSorts().isEmpty()) {
            where.orderBy(query.getSorts().stream().map(SORT_ORDERING_FUNCTION).toArray(Ordering[]::new));
        }
        List<Clause> clauses = new ArrayList<>();
        createClause(query.getCondition(), clauses);
        clauses.forEach(where::and);
        return where;
    }

    public static BuiltStatement delete(ColumnDeleteQuery query, String keySpace) {

        if (Objects.isNull(query.getCondition())) {
            return QueryBuilder.delete().all().from(keySpace, query.getColumnFamily());
        }
        Delete.Where where = QueryBuilder.delete().all().from(keySpace, query.getColumnFamily()).where();
        List<Clause> clauses = new ArrayList<>();
        createClause(query.getCondition(), clauses);
        clauses.forEach(where::and);
        return where;
    }

    public static String count(String columnFamily, String keyspace) {
        return String.format("select count(*) from %s.%s", keyspace, columnFamily);
    }

    private static void createClause(Optional<ColumnCondition> columnConditionOptional, List<Clause> clauses) {
        if (!columnConditionOptional.isPresent()) {
            return;
        }
        ColumnCondition columnCondition = columnConditionOptional.get();
        Column column = columnCondition.getColumn();
        Condition condition = columnCondition.getCondition();
        Object value = column.getValue().get();
        switch (condition) {
            case EQUALS:
                clauses.add(QueryBuilder.eq(getName(column), value));
                return;
            case GREATER_THAN:
                clauses.add(QueryBuilder.gt(getName(column), value));
                return;
            case GREATER_EQUALS_THAN:
                clauses.add(QueryBuilder.gte(getName(column), value));
                return;
            case LESSER_THAN:
                clauses.add(QueryBuilder.lt(getName(column), value));
                return;
            case LESSER_EQUALS_THAN:
                clauses.add(QueryBuilder.lte(getName(column), value));
                return;
            case IN:
                clauses.add(QueryBuilder.in(getName(column), getIinValue(value)));
                return;
            case LIKE:
                clauses.add(QueryBuilder.like(getName(column), value));
                return;
            case AND:
                for (ColumnCondition cc : column.get(new TypeReference<List<ColumnCondition>>() {
                })) {
                    createClause(Optional.of(cc), clauses);
                }
                return;
            case OR:
            default:
                throw new UnsupportedOperationException("The columnCondition " + condition +
                        " is not supported in cassandra column driver");
        }
    }

    private static String getName(Column column) {

        String name = column.getName();
        if (name.charAt(0) == '_') {
            return "\"" + name + "\"";
        }
        return name;
    }

    private static Object[] getIinValue(Object value) {
        if (Iterable.class.isInstance(value)) {
            Iterable values = Iterable.class.cast(value);
            return StreamSupport.stream(values.spliterator(), false).toArray(Object[]::new);
        }
        return new Object[]{value};
    }



}
