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
 *   Alessandro Moscatelli
 */
package org.eclipse.jnosql.databases.mongodb.mapping;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.bson.conversions.Bson;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.databases.mongodb.communication.MongoDBDocumentManager;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableAutoWeld
@AddPackages(value = {Converters.class, DocumentEntityConverter.class})
@AddPackages(Music.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
class DefaultMongoDBTemplateTest {

    @Inject
    private DocumentEntityConverter converter;

    @Inject
    private DocumentEventPersistManager persistManager;

    @Inject
    private EntitiesMetadata entities;

    @Inject
    private Converters converters;

    private MongoDBTemplate template;

    private MongoDBDocumentManager manager;

    @BeforeEach
    public void setUp() {
        this.manager = mock(MongoDBDocumentManager.class);
        Instance instance = mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultMongoDBTemplate(instance, converter, entities, converters, persistManager);
    }

    @Test
    public void shouldReturnErrorOnDeleteMethod() {
        assertThrows(NullPointerException.class, () -> template.delete((String) null, null));
        assertThrows(NullPointerException.class, () -> template.delete("Collection", null));
        assertThrows(NullPointerException.class, () -> template.delete((String) null,
                eq("name", "Poliana")));

        assertThrows(NullPointerException.class, () -> template.delete(Person.class, null));
        assertThrows(NullPointerException.class, () -> template.delete((Class<Object>) null,
                eq("name", "Poliana")));
    }

    @Test
    public void shouldDeleteWithCollectionName() {
        Bson filter = eq("name", "Poliana");
        template.delete("Person", filter);
        Mockito.verify(manager).delete("Person", filter);
    }

    @Test
    public void shouldDeleteWithEntity() {
        Bson filter = eq("name", "Poliana");
        template.delete(Person.class, filter);
        Mockito.verify(manager).delete("Person", filter);
    }

    @Test
    public void shouldReturnErrorOnSelectMethod() {
        assertThrows(NullPointerException.class, () -> template.select((String) null, null));
        assertThrows(NullPointerException.class, () -> template.select("Collection", null));
        assertThrows(NullPointerException.class, () -> template.select((String) null,
                eq("name", "Poliana")));

        assertThrows(NullPointerException.class, () -> template.select(Person.class, null));
        assertThrows(NullPointerException.class, () -> template.select((Class<Object>) null,
                eq("name", "Poliana")));
    }

    @Test
    public void shouldSelectWithCollectionName() {
        DocumentEntity entity = DocumentEntity.of("Person", Arrays
                .asList(Document.of("_id", "Poliana"),
                        Document.of("age", 30)));
        Bson filter = eq("name", "Poliana");
        Mockito.when(manager.select("Person", filter))
                .thenReturn(Stream.of(entity));
        Stream<Person> stream = template.select("Person", filter);
        Assertions.assertNotNull(stream);
        Person poliana = stream.findFirst()
                .orElseThrow(() -> new IllegalStateException("There is an issue on the test"));

        Assertions.assertNotNull(poliana);
        assertEquals("Poliana", poliana.getName());
        assertEquals(30, poliana.getAge());
    }

    @Test
    public void shouldSelectWithEntity() {
        DocumentEntity entity = DocumentEntity.of("Person", Arrays
                .asList(Document.of("_id", "Poliana"),
                        Document.of("age", 30)));
        Bson filter = eq("name", "Poliana");
        Mockito.when(manager.select("Person", filter))
                .thenReturn(Stream.of(entity));
        Stream<Person> stream = template.select(Person.class, filter);
        Assertions.assertNotNull(stream);
        Person poliana = stream.findFirst()
                .orElseThrow(() -> new IllegalStateException("There is an issue on the test"));

        Assertions.assertNotNull(poliana);
        assertEquals("Poliana", poliana.getName());
        assertEquals(30, poliana.getAge());
    }

    @Test
    public void shouldReturnErrorOnAggregateMethod() {
        assertThrows(NullPointerException.class, () -> template.aggregate((String) null, (List) null));
        assertThrows(NullPointerException.class, () -> template.aggregate("Collection", (List) null));
        assertThrows(NullPointerException.class, () -> template.aggregate((String) null,
                Collections.singletonList(eq("name", "Poliana"))));

        assertThrows(NullPointerException.class, () -> template.aggregate(Person.class, (List) null));
        assertThrows(NullPointerException.class, () -> template.aggregate((Class<Object>) null,
                Collections.singletonList(eq("name", "Poliana"))));
    }

    @Test
    public void shouldAggregateWithCollectionName() {
        Bson[] predicates = {
                Aggregates.match(eq("name", "Poliana")),
                Aggregates.group("$stars", Accumulators.sum("count", 1))
        };

        template.aggregate("Person", predicates);
        Mockito.verify(manager).aggregate("Person", predicates);
    }

    @Test
    public void shouldAggregateWithEntity() {
        Bson[] predicates = {
                Aggregates.match(eq("name", "Poliana")),
                Aggregates.group("$stars", Accumulators.sum("count", 1))
        };

        template.aggregate(Person.class, predicates);
        Mockito.verify(manager).aggregate("Person", predicates);
    }

    @Test
    public void shouldCountByFilterWithCollectionName() {
        var filter = eq("name", "Poliana");

        template.count("Person", filter);

        Mockito.verify(manager).count("Person", filter);
    }

    @Test
    public void shouldCountByFilterWithEntity() {
        var filter = eq("name", "Poliana");

        template.count(Person.class, filter);

        Mockito.verify(manager).count("Person", filter);
    }

    @Test
    public void shouldReturnErrorOnCountByFilterMethod() {
        var filter = eq("name", "Poliana");
        assertThrows(NullPointerException.class, () -> template.count((String) null, null));
        assertThrows(NullPointerException.class, () -> template.count((String) null, filter));
        assertThrows(NullPointerException.class, () -> template.count("Person", null));
        assertThrows(NullPointerException.class, () -> template.count((Class<Person>) null, null));
        assertThrows(NullPointerException.class, () -> template.count((Class<Person>) null, filter));
        assertThrows(NullPointerException.class, () -> template.count(Person.class, null));
    }

}
