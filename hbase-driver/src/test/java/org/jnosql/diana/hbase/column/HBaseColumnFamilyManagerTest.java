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

import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnFamilyManagerFactory;
import org.jnosql.diana.api.column.ColumnQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.jnosql.diana.api.column.ColumnCondition.eq;
import static org.jnosql.diana.api.column.query.ColumnQueryBuilder.delete;
import static org.jnosql.diana.api.column.query.ColumnQueryBuilder.select;
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
        columnFamilyManager.insert(entity);
    }

    @Test(expected = DianaHBaseException.class)
    public void shouldReturnErrorWhenKeyIsNotDefined() {
        ColumnEntity entity = ColumnEntity.of(FAMILY);
        entity.add(Column.of("id", "otaviojava"));
        entity.add(Column.of("age", 26));
        entity.add(Column.of("country", "Brazil"));
        columnFamilyManager.insert(entity);
    }

    @Test
    public void shouldFind() {
        columnFamilyManager.insert(createEntity());

        ColumnQuery query = select().from(FAMILY).where(eq(Column.of(ID_FIELD, "otaviojava"))).build();
        List<ColumnEntity> columnFamilyEntities = columnFamilyManager.select(query);
        assertNotNull(columnFamilyEntities);
        assertFalse(columnFamilyEntities.isEmpty());
        ColumnEntity entity = columnFamilyEntities.get(0);
        assertEquals(FAMILY, entity.getName());
        assertThat(entity.getColumns(), containsInAnyOrder(Column.of(ID_FIELD, "otaviojava"),
                Column.of("age", "26"), Column.of("country", "Brazil")));
    }

    @Test
    public void shouldFindInBatch() {
        columnFamilyManager.insert(createEntity());
        columnFamilyManager.insert(createEntity2());

        ColumnQuery query = select().from(FAMILY).where(eq(Column.of(ID_FIELD, "otaviojava")))
                .and(eq(Column.of(ID_FIELD, "poliana"))).build();

        List<ColumnEntity> entities = columnFamilyManager.select(query);
        assertEquals(Integer.valueOf(2), Integer.valueOf(entities.size()));

    }

    @Test
    public void shouldDeleteEntity() {
        columnFamilyManager.insert(createEntity());
        ColumnQuery query = select().from(FAMILY).where(eq(Column.of(ID_FIELD, "otaviojava"))).build();
        ColumnDeleteQuery deleteQuery = delete().from(FAMILY).where(eq(Column.of(ID_FIELD, "otaviojava"))).build();
        columnFamilyManager.delete(deleteQuery);
        List<ColumnEntity> entities = columnFamilyManager.select(query);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldDeleteEntities() {
        columnFamilyManager.insert(createEntity());
        columnFamilyManager.insert(createEntity2());

        ColumnQuery query = select().from(FAMILY).where(eq(Column.of(ID_FIELD, "otaviojava")))
                .and(eq(Column.of(ID_FIELD, "poliana"))).build();

        ColumnDeleteQuery deleteQuery = delete().from(FAMILY).where(eq(Column.of(ID_FIELD, "otaviojava")))
                .and(eq(Column.of(ID_FIELD, "poliana"))).build();

        columnFamilyManager.delete(deleteQuery);
        List<ColumnEntity> entities = columnFamilyManager.select(query);
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