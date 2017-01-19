/*
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
package org.jnosql.diana.hbase.column;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnCondition;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnQuery;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

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
    public ColumnEntity save(ColumnEntity entity) {
        String family = entity.getName();
        List<Column> columns = entity.getColumns();
        if (columns.isEmpty()) {
            return entity;
        }
        Column columnID = entity.find(HBaseUtils.KEY_COLUMN).orElseThrow(() -> new DianaHBaseException(KEY_REQUIRED_ERROR));

        Put put = new Put(Bytes.toBytes(valueToString(columnID.getValue())));
        columns.stream().filter(Predicate.isEqual(columnID).negate()).forEach(column -> {
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column.getName()), Bytes
                    .toBytes(valueToString(column.getValue())));
        });
        try {
            table.put(put);
        } catch (IOException e) {
            throw new DianaHBaseException("An error happened when try to save an entity", e);
        }
        return entity;
    }

    @Override
    public ColumnEntity save(ColumnEntity entity, Duration ttl) throws NullPointerException {
        throw new UnsupportedOperationException("There is not support to save async");
    }


    @Override
    public void delete(ColumnQuery query) {

        ColumnCondition condition = query.getCondition();
        if (isQuerySupported(condition)) {
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
                throw new DianaHBaseException("An error when try to delete columns", e);
            }
        }

    }


    @Override
    public List<ColumnEntity> find(ColumnQuery query) {

        ColumnCondition condition = query.getCondition();
        if (isQuerySupported(condition)) {
            return Stream.of(findById(condition)).map(EntityUnit::new).filter(EntityUnit::isNotEmpty).map(EntityUnit::toEntity).collect(toList());
        }

        throw new UnsupportedOperationException("There is not support to find more than one key");

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
            throw new DianaHBaseException("An error when try to find by id", e);
        }
    }

    private Map<String, List<Column>> toMap(Result result) {
        Map<String, List<Column>> columnsByKey = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            String key = new String(CellUtil.cloneRow(cell));
            String name = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            List<Column> columns = columnsByKey.getOrDefault(key, new ArrayList<>());
            columns.add(Column.of(name, value));
            columnsByKey.put(key, columns);
        }
        return columnsByKey;
    }


    private void convert(ColumnCondition columnCondition, List<String> values) {
        Condition condition = columnCondition.getCondition();

        if (Condition.AND.equals(condition)) {
            columnCondition.getColumn().get(new TypeReference<List<ColumnCondition>>() {
            }).forEach(c -> convert(c, values));
        }
        if (Condition.IN.equals(condition)) {
            values.addAll(columnCondition.getColumn().get(new TypeReference<List<String>>() {
            }));
        } else if (Condition.EQUALS.equals(condition)) {
            values.add(valueToString(columnCondition.getColumn().getValue()));
        }


    }

    private boolean isQuerySupported(ColumnCondition columnCondition) {

        Condition c = columnCondition.getCondition();
        if (Condition.AND.equals(c)) {
            List<ColumnCondition> columnConditions = columnCondition.getColumn().get(new TypeReference<List<ColumnCondition>>() {
            });
            for (ColumnCondition cc : columnConditions) {
                isQuerySupported(cc);
            }

        }
        return Condition.EQUALS.equals(c) || Condition.IN.equals(c);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HBaseColumnFamilyManager{");
        sb.append("connection=").append(connection);
        sb.append('}');
        return sb.toString();
    }


}
