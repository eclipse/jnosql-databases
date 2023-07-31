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
package org.eclipse.jnosql.databases.cassandra.mapping;


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.column.ColumnEntityConverter;
import org.eclipse.jnosql.mapping.column.ColumnFieldValue;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.metadata.FieldMetadata;
import org.eclipse.jnosql.mapping.metadata.GenericFieldMetadata;

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
    protected <T> Consumer<String> feedObject(T instance, List<Column> columns, Map<String, FieldMetadata> fieldsGroupByName) {
        return k -> {
            FieldMetadata field = fieldsGroupByName.get(k);
            if (field.value(UDT.class).isPresent()) {
                Optional<Column> column = columns.stream().filter(c -> c.name().equals(k)).findFirst();
                setUDTField(instance, column, field);
            } else {
                super.feedObject(instance, columns, fieldsGroupByName).accept(k);
            }
        };
    }

    private <T> void setUDTField(T instance, Optional<Column> column, FieldMetadata field) {
        if (column.isPresent() && org.eclipse.jnosql.databases.cassandra.communication.UDT.class.isInstance(column.get())) {
            org.eclipse.jnosql.databases.cassandra.communication.UDT udt =
                    org.eclipse.jnosql.databases.cassandra.communication.UDT.class.cast(column.get());
            Object columns = udt.get();
            if (StreamSupport.stream(Iterable.class.cast(columns).spliterator(), false)
                    .allMatch(Iterable.class::isInstance)) {
                GenericFieldMetadata genericField = GenericFieldMetadata.class.cast(field);
                Collection collection = genericField.collectionInstance();
                List<List<Column>> embeddable = (List<List<Column>>) columns;
                for (List<Column> columnList : embeddable) {
                    Object element = toEntity(genericField.elementType(), columnList);
                    collection.add(element);
                }
                field.write(instance, collection);
            } else {
                Object value = toEntity(field.type(), (List<Column>) columns);
                field.write(instance, value);
            }
        }
    }

    @Override
    protected ColumnFieldValue to(FieldMetadata field, Object entityInstance) {

        Object value = field.read(entityInstance);
        Optional<String> annotation = field.value(UDT.class);
        return annotation.<ColumnFieldValue>map(v -> new CassandraUDTType(v, value, field))
                .orElseGet( () -> super.to(field, entityInstance));
    }
}