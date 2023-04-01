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

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.communication.column.ColumnDeleteQuery;
import org.eclipse.jnosql.communication.column.ColumnEntity;
import org.eclipse.jnosql.communication.column.ColumnManager;
import org.eclipse.jnosql.communication.column.ColumnManagerFactory;
import org.eclipse.jnosql.communication.column.ColumnQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;
import java.util.stream.Collectors;

import static org.eclipse.jnosql.communication.column.ColumnDeleteQuery.delete;
import static org.eclipse.jnosql.communication.column.ColumnQuery.select;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION_MATCHES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = INTEGRATION, matches = INTEGRATION_MATCHES)
public class HBaseFamilyManagerTest {

    private static final String DATA_BASE = "database";
    public static final String FAMILY = "person";
    public static final String ID_FIELD = HBaseUtils.KEY_COLUMN;

    private ColumnManagerFactory managerFactory;

    private ColumnManager manager;

    @BeforeEach
    public void setUp() {
        HBaseColumnConfiguration configuration = new HBaseColumnConfiguration();
        configuration.add(FAMILY);
        managerFactory = configuration.apply(Settings.builder().build());
        manager = managerFactory.apply(DATA_BASE);
    }


    @Test
    public void shouldSave() {
        ColumnEntity entity = createEntity();
        manager.insert(entity);
    }

    @Test
    public void shouldReturnErrorWhenKeyIsNotDefined() {
        ColumnEntity entity = ColumnEntity.of(FAMILY);
        entity.add(Column.of("id", "otaviojava"));
        entity.add(Column.of("age", 26));
        entity.add(Column.of("country", "Brazil"));
        assertThrows(HBaseException.class, () -> manager.insert(entity));
    }

    @Test
    public void shouldFind() {
        manager.insert(createEntity());

        ColumnQuery query = select().from(FAMILY).where(ID_FIELD).eq("otaviojava").build();
        List<ColumnEntity> columnFamilyEntities = manager.select(query).collect(Collectors.toList());
        assertNotNull(columnFamilyEntities);
        assertFalse(columnFamilyEntities.isEmpty());
        ColumnEntity entity = columnFamilyEntities.get(0);
        assertEquals(FAMILY, entity.name());
        assertThat(entity.columns()).contains(Column.of(ID_FIELD, "otaviojava"),
                Column.of("age", "26"), Column.of("country", "Brazil"));
    }

    @Test
    public void shouldFindInBatch() {
        manager.insert(createEntity());
        manager.insert(createEntity2());

        ColumnQuery query = select().from(FAMILY).where(ID_FIELD).eq("otaviojava")
                .or(ID_FIELD).eq("poliana").build();

        List<ColumnEntity> entities = manager.select(query).collect(Collectors.toList());
        assertEquals(Integer.valueOf(2), Integer.valueOf(entities.size()));

    }

    @Test
    public void shouldDeleteEntity() {
        manager.insert(createEntity());
        ColumnQuery query = select().from(FAMILY).where(ID_FIELD).eq("otaviojava").build();
        ColumnDeleteQuery deleteQuery = delete().from(FAMILY).where(ID_FIELD).eq("otaviojava").build();
        manager.delete(deleteQuery);
        List<ColumnEntity> entities = manager.select(query).collect(Collectors.toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldDeleteEntities() {
        manager.insert(createEntity());
        manager.insert(createEntity2());

        ColumnQuery query = select().from(FAMILY).where(ID_FIELD).eq("otaviojava")
                .or(ID_FIELD).eq("poliana").build();

        ColumnDeleteQuery deleteQuery = delete().from(FAMILY).where(ID_FIELD).eq("otaviojava")
                .or(ID_FIELD).eq("poliana").build();

        manager.delete(deleteQuery);
        List<ColumnEntity> entities = manager.select(query).collect(Collectors.toList());
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