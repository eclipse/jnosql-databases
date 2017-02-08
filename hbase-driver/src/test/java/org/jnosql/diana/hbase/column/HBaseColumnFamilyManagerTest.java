/*
 * Copyright 2017 Eclipse Foundation
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

import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnCondition;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnFamilyManagerFactory;
import org.jnosql.diana.api.column.ColumnQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class HBaseColumnFamilyManagerTest {

    private static final String DATA_BASE = "database";
    public static final String FAMILY = "person";
    public static final String ID_FIELD = HBaseUtils.KEY_COLUMN;

    private ColumnFamilyManagerFactory managerFactory;

    private ColumnFamilyManager columnFamilyManager;

    @Before
    public void setUp() {
        HBaseColumnConfiguration configuration = new HBaseColumnConfiguration();
        configuration.add(FAMILY);
        managerFactory = configuration.get();
        columnFamilyManager = managerFactory.get(DATA_BASE);
    }


    @Test
    public void shouldSave() {
        ColumnEntity entity = createEntity();
        columnFamilyManager.save(entity);
    }

    @Test(expected = DianaHBaseException.class)
    public void shouldReturnErrorWhenKeyIsNotDefined() {
        ColumnEntity entity = ColumnEntity.of(FAMILY);
        entity.add(Column.of("id", "otaviojava"));
        entity.add(Column.of("age", 26));
        entity.add(Column.of("country", "Brazil"));
        columnFamilyManager.save(entity);
    }

    @Test
    public void shouldFind() {
        columnFamilyManager.save(createEntity());
        ColumnQuery query = ColumnQuery.of(FAMILY);
        query.and(ColumnCondition.eq(Column.of(ID_FIELD, "otaviojava")));
        List<ColumnEntity> columnFamilyEntities = columnFamilyManager.find(query);
        assertNotNull(columnFamilyEntities);
        assertFalse(columnFamilyEntities.isEmpty());
        ColumnEntity entity = columnFamilyEntities.get(0);
        assertEquals(FAMILY, entity.getName());
        assertThat(entity.getColumns(), containsInAnyOrder(Column.of(ID_FIELD, "otaviojava"), Column.of("age", "26"), Column.of("country", "Brazil")));
    }

    @Test
    public void shouldFindInBatch() {
        columnFamilyManager.save(createEntity());
        columnFamilyManager.save(createEntity2());

        ColumnQuery query = ColumnQuery.of(FAMILY);
        query.and(ColumnCondition.eq(Column.of(ID_FIELD, "otaviojava")));
        query.and(ColumnCondition.eq(Column.of(ID_FIELD, "poliana")));
        List<ColumnEntity> entities = columnFamilyManager.find(query);
        assertEquals(Integer.valueOf(2), Integer.valueOf(entities.size()));

    }

    @Test
    public void shouldDeleteEntity() {
        columnFamilyManager.save(createEntity());
        ColumnQuery query = ColumnQuery.of(FAMILY);
        query.and(ColumnCondition.eq(Column.of(ID_FIELD, "otaviojava")));
        columnFamilyManager.delete(ColumnDeleteQuery.of(query.getColumnFamily(), query.getCondition().get()));
        List<ColumnEntity> entities = columnFamilyManager.find(query);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldDeleteEntities() {
        columnFamilyManager.save(createEntity());
        columnFamilyManager.save(createEntity2());
        ColumnQuery query = ColumnQuery.of(FAMILY);
        query.and(ColumnCondition.eq(Column.of(ID_FIELD, "otaviojava")));
        query.and(ColumnCondition.eq(Column.of(ID_FIELD, "poliana")));
        columnFamilyManager.delete(ColumnDeleteQuery.of(query.getColumnFamily(), query.getCondition().get()));
        List<ColumnEntity> entities = columnFamilyManager.find(query);
        assertTrue(entities.isEmpty());
    }

    private ColumnEntity createEntity() {
        ColumnEntity entity = ColumnEntity.of(FAMILY);
        entity.add(Column.of(ID_FIELD, "otaviojava"));
        entity.add(Column.of("age", 26));
        entity.add(Column.of("country", "Brazil"));
        return entity;
    }

    private ColumnEntity createEntity2() {
        ColumnEntity entity = ColumnEntity.of(FAMILY);
        entity.add(Column.of(ID_FIELD, "poliana"));
        entity.add(Column.of("age", 24));
        entity.add(Column.of("country", "Brazil"));
        return entity;
    }


}