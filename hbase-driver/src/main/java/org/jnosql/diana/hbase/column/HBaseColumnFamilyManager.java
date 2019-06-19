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
package org.jnosql.diana.hbase.column;


import jakarta.nosql.column.ColumnEntity;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import jakarta.nosql.Condition;
import jakarta.nosql.TypeReference;
import jakarta.nosql.Value;
import jakarta.nosql.ValueWriter;
import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnCondition;
import jakarta.nosql.column.ColumnDeleteQuery;
import jakarta.nosql.column.ColumnQuery;
import jakarta.nosql.column.ColumnFamilyManager;
import jakarta.nosql.column.ColumnQuery;
import org.jnosql.diana.writer.ValueWriterDecorator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static jakarta.nosql.Condition.EQUALS;
import static jakarta.nosql.Condition.IN;
import static jakarta.nosql.Condition.OR;
import static org.jnosql.diana.hbase.column.HBaseUtils.KEY_COLUMN;

/**
 * The Hbase implementation to {@link ColumnFamilyManager}.
 * It does not support TTL methods
 * <p>{@link HBaseColumnFamilyManager#insert(ColumnEntity, Duration)}</p>
 */
public class HBaseColumnFamilyManager implements ColumnFamilyManager {

    private static final String KEY_REQUIRED_ERROR = "\"To save an entity is necessary to have an row, a Column that has a blank name. Documents.of(\\\"\\\", keyValue);\"";

    private final Connection connection;
    private final Table table;
    private final ValueWriter writerField = ValueWriterDecorator.getInstance();


    HBaseColumnFamilyManager(Connection connection, Table table) {
        this.connection = connection;
        this.table = table;
    }

    @Override
    public ColumnEntity insert(ColumnEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        String family = entity.getName();
        List<Column> columns = entity.getColumns();
        if (columns.isEmpty()) {
            return entity;
        }
        Column columnID = entity.find(KEY_COLUMN).orElseThrow(() -> new HBaseException(KEY_REQUIRED_ERROR));

        Put put = new Put(Bytes.toBytes(valueToString(columnID.getValue())));
        columns.stream().filter(Predicate.isEqual(columnID).negate()).forEach(column ->
                put.addColumn(Bytes.toBytes(family),
                        Bytes.toBytes(column.getName()),
                        Bytes.toBytes(valueToString(column.getValue()))));
        try {
            table.put(put);
        } catch (IOException e) {
            throw new HBaseException("An error happened when try to save an entity", e);
        }
        return entity;
    }

    @Override
    public ColumnEntity update(ColumnEntity entity) throws NullPointerException {
        return insert(entity);
    }

    @Override
    public Iterable<ColumnEntity> update(Iterable<ColumnEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update)
                .collect(Collectors.toList());
    }

    @Override
    public ColumnEntity insert(ColumnEntity entity, Duration ttl) throws NullPointerException {
        throw new UnsupportedOperationException("There is not support to save async");
    }

    @Override
    public Iterable<ColumnEntity> insert(Iterable<ColumnEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<ColumnEntity> insert(Iterable<ColumnEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> insert(e, ttl))
                .collect(Collectors.toList());
    }


    @Override
    public void delete(ColumnDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        ColumnCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        checkedCondition(condition);
        List<String> values = new ArrayList<>();

        convert(condition, values);
        List<Delete> deletes = values
                .stream()
                .map(String::getBytes)
                .map(Delete::new)
                .collect(toList());
        try {
            table.delete(deletes);
        } catch (IOException e) {
            throw new HBaseException("An error when try to delete columns", e);
        }

    }


    @Override
    public List<ColumnEntity> select(ColumnQuery query) {
        Objects.requireNonNull(query, "query is required");
        ColumnCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        checkedCondition(condition);
        return Stream.of(findById(condition)).map(EntityUnit::new).filter(EntityUnit::isNotEmpty).map(EntityUnit::toEntity).collect(toList());
    }

    @Override
    public long count(String columnFamily) {
        throw new UnsupportedOperationException("Hbase does not have support to count method");
    }


    @Override
    public void close() {
        try {
            connection.close();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String valueToString(Value value) {
        Object object = value.get();
        if (writerField.isCompatible(object.getClass())) {
            return writerField.write(object).toString();
        } else {
            return object.toString();
        }
    }

    private Result[] findById(ColumnCondition condition) {
        List<String> values = new ArrayList<>();
        convert(condition, values);

        List<Get> gets = values.stream()
                .map(String::getBytes)
                .map(Get::new).collect(toList());
        try {
            return table.get(gets);
        } catch (IOException e) {
            throw new HBaseException("An error when try to find by id", e);
        }
    }


    private void convert(ColumnCondition columnCondition, List<String> values) {
        Condition condition = columnCondition.getCondition();

        if (OR.equals(condition)) {
            columnCondition.getColumn().get(new TypeReference<List<ColumnCondition>>() {
            }).forEach(c -> convert(c, values));
        } else if (IN.equals(condition)) {
            values.addAll(columnCondition.getColumn().get(new TypeReference<List<String>>() {
            }));
        } else if (EQUALS.equals(condition)) {
            values.add(valueToString(columnCondition.getColumn().getValue()));
        }


    }

    private void checkedCondition(ColumnCondition columnCondition) {

        Condition condition = columnCondition.getCondition();
        if (OR.equals(condition)) {
            List<ColumnCondition> columnConditions = columnCondition.getColumn().get(new TypeReference<List<ColumnCondition>>() {
            });
            for (ColumnCondition cc : columnConditions) {
                checkedCondition(cc);
            }
            return;

        }
        if (!EQUALS.equals(condition) && !IN.equals(condition)) {
            throw new UnsupportedOperationException("Hbase does not support the following condition: %s just AND, EQUAL and IN ");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HBaseColumnFamilyManager{");
        sb.append("connection=").append(connection);
        sb.append('}');
        return sb.toString();
    }


}
