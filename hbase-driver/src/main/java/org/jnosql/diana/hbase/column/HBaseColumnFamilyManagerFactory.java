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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.jnosql.diana.api.column.ColumnFamilyManagerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseColumnFamilyManagerFactory implements ColumnFamilyManagerFactory<HBaseColumnFamilyManager> {

    private final Configuration configuration;

    private final List<String> families;

    HBaseColumnFamilyManagerFactory(Configuration configuration, List<String> families) {
        this.configuration = configuration;
        this.families = families;
    }

    @Override
    public HBaseColumnFamilyManager get(String database) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(database);
            if (admin.tableExists(tableName)) {
                existTable(admin, tableName);
            } else {
                createTable(admin, tableName);
            }
            Table table = connection.getTable(tableName);
            return new HBaseColumnFamilyManager(connection, table);
        } catch (IOException e) {
            throw new DianaHBaseException("A error happened when try to create ColumnFamilyManager", e);
        }
    }


    private void existTable(Admin admin, TableName tableName) throws IOException {
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        List<String> familiesExist = Arrays.stream(columnFamilies).map(HColumnDescriptor::getName).map(String::new).collect(Collectors.toList());
        families.stream().filter(s -> !familiesExist.contains(s)).map(HColumnDescriptor::new).forEach(tableDescriptor::addFamily);
        admin.modifyTable(tableName, tableDescriptor);
    }

    private void createTable(Admin admin, TableName tableName) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        families.stream().map(HColumnDescriptor::new).forEach(desc::addFamily);
        admin.createTable(desc);
    }

    @Override
    public void close() {

    }


}
