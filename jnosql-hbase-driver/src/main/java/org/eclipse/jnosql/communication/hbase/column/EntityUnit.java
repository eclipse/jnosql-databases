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
package org.eclipse.jnosql.communication.hbase.column;


import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnEntity;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.CellUtil.cloneFamily;
import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneRow;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;
import static org.eclipse.jnosql.communication.hbase.column.HBaseUtils.getKey;

class EntityUnit {

    private String rowKey;

    private String columnFamily;

    private final List<Column> columns = new ArrayList<>();

    EntityUnit(Result result) {

        for (Cell cell : result.rawCells()) {

            String name = new String(cloneQualifier(cell));
            String value = new String(cloneValue(cell));
            if (this.rowKey == null) {
                this.rowKey = new String(cloneRow(cell));
            }
            if (this.columnFamily == null) {
                this.columnFamily = new String(cloneFamily(cell));
            }
            columns.add(Column.of(name, value));
        }
    }


    public boolean isNotEmpty() {
        return !columns.isEmpty();
    }

    public ColumnEntity toEntity() {
        ColumnEntity entity = ColumnEntity.of(columnFamily);
        entity.addAll(columns);
        entity.add(getKey(rowKey));
        return entity;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EntityUnit{");
        sb.append("rowKey='").append(rowKey).append('\'');
        sb.append(", columnFamily='").append(columnFamily).append('\'');
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
