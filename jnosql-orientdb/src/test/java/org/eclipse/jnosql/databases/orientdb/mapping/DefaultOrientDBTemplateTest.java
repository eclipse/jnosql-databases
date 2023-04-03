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
package org.eclipse.jnosql.databases.orientdb.mapping;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBDocumentManager;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBLiveCallback;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBLiveCreateCallback;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.document.DocumentWorkflow;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Convert.class,
        DocumentEntityConverter.class, SQL.class})
@AddPackages(MockProducer.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class, OrientDBExtension.class})
public class DefaultOrientDBTemplateTest {

    @Inject
    private DocumentEntityConverter converter;

    @Inject
    private DocumentWorkflow flow;

    @Inject
    private DocumentEventPersistManager persistManager;

    @Inject
    private EntitiesMetadata entities;

    @Inject
    private Converters converters;

    private OrientDBDocumentManager manager;

    private OrientDBTemplate template;


    @BeforeEach
    public void setup() {
        manager = Mockito.mock(OrientDBDocumentManager.class);
        Instance instance = Mockito.mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultOrientDBTemplate(instance, converter, flow, persistManager, entities, converters);

        DocumentEntity entity = DocumentEntity.of("Person");
        entity.add(Document.of("name", "Ada"));
        entity.add(Document.of("age", 10));
        when(manager.sql(Mockito.anyString(), Mockito.any(String.class)))
                .thenReturn(Stream.of(entity));
    }

    @Test
    public void shouldFindQuery() {
        Stream<Person> people = template.sql("sql * from Person where name = ?", "Ada");

        assertThat(people.collect(Collectors.toList())).contains(new Person("Ada", 10));
        verify(manager).sql(Mockito.eq("sql * from Person where name = ?"), Mockito.eq("Ada"));
    }

    @Test
    public void shouldLive() {

        DocumentQuery query = select().from("Person").build();

        OrientDBLiveCreateCallback<Person> callBack = p -> {
        };
        template.live(query, OrientDBLiveCallbackBuilder.builder().onCreate(callBack).build());
        verify(manager).live(Mockito.eq(query), Mockito.any(OrientDBLiveCallback.class));
    }

    @Test
    public void shouldLiveQuery() {
        OrientDBLiveCreateCallback<Person> callBack = p -> {
        };
        template.live("sql from Person where name = ?", OrientDBLiveCallbackBuilder.builder().onCreate(callBack).build(), "Ada");
        verify(manager).live(Mockito.eq("sql from Person where name = ?"),
                Mockito.any(OrientDBLiveCallback.class),
                Mockito.eq("Ada"));
    }
}