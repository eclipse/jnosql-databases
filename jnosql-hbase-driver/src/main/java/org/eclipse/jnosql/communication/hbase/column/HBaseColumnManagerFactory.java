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
package org.eclipse.jnosql.communication.hbase.column;


import jakarta.nosql.column.ColumnManagerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseColumnManagerFactory implements ColumnManagerFactory {

    private final Configuration configuration;

    private final List<String> families;

    HBaseColumnManagerFactory(Configuration configuration, List<String> families) {
        this.configuration = configuration;
        this.families = families;
    }

    @Override
    public HBaseColumnManager apply(String database) {
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
            return new HBaseColumnManager(connection, table, database);
        } catch (IOException e) {
            throw new HBaseException("A error happened when try to create ColumnManager", e);
        }
    }


    private void existTable(Admin admin, TableName tableName) throws IOException {
        TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
        ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        final TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        List<String> familiesExist = Arrays.stream(columnFamilies).map(ColumnFamilyDescriptor::getName).map(String::new).collect(Collectors.toList());
        if (familiesExist.size() != families.size()) {
            families.stream().filter(s -> !familiesExist.contains(s))
                    .forEach(s -> builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(s.getBytes())
                            .build()));
            final TableDescriptor descriptor = builder.build();
            admin.modifyTable(descriptor);
        }

    }

    private void createTable(Admin admin, TableName tableName) throws IOException {
        final TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        families.stream().forEach(s -> builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(s.getBytes())
                .build()));
        final TableDescriptor descriptor = builder.build();
        admin.createTable(descriptor);
    }

    @Override
    public void close() {

    }


}
