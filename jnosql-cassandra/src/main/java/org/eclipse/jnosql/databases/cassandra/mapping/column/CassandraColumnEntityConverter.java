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
package org.eclipse.jnosql.databases.cassandra.mapping.column;


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.column.ColumnEntityConverter;
import org.eclipse.jnosql.mapping.column.ColumnFieldValue;
import org.eclipse.jnosql.mapping.reflection.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.FieldMapping;
import org.eclipse.jnosql.mapping.reflection.GenericFieldMapping;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

@ApplicationScoped
@Typed(CassandraColumnEntityConverter.class)
class CassandraColumnEntityConverter extends ColumnEntityConverter {

    @Inject
    private EntitiesMetadata entities;

    @Inject
    private Converters converters;


    @Override
    protected EntitiesMetadata getEntities() {
        return entities;
    }

    @Override
    protected Converters getConverters() {
        return converters;
    }

    @Override
    protected <T> Consumer<String> feedObject(T instance, List<Column> columns, Map<String, FieldMapping> fieldsGroupByName) {
        return k -> {
            FieldMapping field = fieldsGroupByName.get(k);
            if (Objects.nonNull(field.nativeField().getAnnotation(UDT.class))) {
                Optional<Column> column = columns.stream().filter(c -> c.name().equals(k)).findFirst();
                setUDTField(instance, column, field);
            } else {
                super.feedObject(instance, columns, fieldsGroupByName).accept(k);
            }
        };
    }

    private <T> void setUDTField(T instance, Optional<Column> column, FieldMapping field) {
        if (column.isPresent() && org.eclipse.jnosql.databases.cassandra.communication.column.UDT.class.isInstance(column.get())) {
            org.eclipse.jnosql.databases.cassandra.communication.column.UDT udt =
                    org.eclipse.jnosql.databases.cassandra.communication.column.UDT.class.cast(column.get());
            Object columns = udt.get();
            if (StreamSupport.stream(Iterable.class.cast(columns).spliterator(), false)
                    .allMatch(Iterable.class::isInstance)) {
                GenericFieldMapping genericField = GenericFieldMapping.class.cast(field);
                Collection collection = genericField.getCollectionInstance();
                List<List<Column>> embeddable = (List<List<Column>>) columns;
                for (List<Column> columnList : embeddable) {
                    Object element = toEntity(genericField.getElementType(), columnList);
                    collection.add(element);
                }
                field.write(instance, collection);
            } else {
                Object value = toEntity(field.nativeField().getType(), (List<Column>) columns);
                field.write(instance, value);
            }
        }
    }

    @Override
    protected ColumnFieldValue to(FieldMapping field, Object entityInstance) {

        Object value = field.read(entityInstance);
        UDT annotation = field.nativeField().getAnnotation(UDT.class);
        if (Objects.isNull(annotation)) {
            return super.to(field, entityInstance);
        } else {
            return new CassandraUDTType(annotation.value(), value, field);
        }
    }
}