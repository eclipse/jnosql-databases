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
package org.eclipse.jnosql.mapping.elasticsearch.document;

import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.util.ObjectBuilder;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.elasticsearch.document.ElasticsearchDocumentManager;
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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Convert.class,
        DocumentEntityConverter.class, ElasticsearchTemplate.class})
@AddPackages(Person.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
public class DefaultElasticsearchTemplateTest {

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

    private ElasticsearchDocumentManager manager;

    private DefaultElasticsearchTemplate template;


    @BeforeEach
    public void setup() {
        manager = Mockito.mock(ElasticsearchDocumentManager.class);
        Instance instance = Mockito.mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultElasticsearchTemplate(instance, converter, flow, persistManager, entities, converters);

        DocumentEntity entity = DocumentEntity.of("Person");
        entity.add(Document.of("name", "Ada"));
        entity.add(Document.of("age", 10));
        when(manager.search(Mockito.any(SearchRequest.class)))
                .thenReturn(Stream.of(entity));
    }

    @Test
    public void shouldFindQuery() {
        String searchText = "bike";
        Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> fn  =
                q -> q.query(t -> t.match(MatchQuery.of(m -> m.field("field").query(2))));
        SearchRequest request = fn.apply(new SearchRequest.Builder()).build();
        List<Person> people = template.<Person>search(request).collect(Collectors.toList());

        assertThat(people).contains(new Person("Ada", 10));
        Mockito.verify(manager).search(Mockito.eq(request));
    }

    @Test
    public void shouldGetConverter() {
        assertNotNull(template.getConverter());
        assertEquals(converter, template.getConverter());
    }

    @Test
    public void shouldGetManager() {
        assertNotNull(template.getManager());
        assertEquals(manager, template.getManager());
    }


    @Test
    public void shouldGetWorkflow() {
        assertNotNull(template.getWorkflow());
        assertEquals(flow, template.getWorkflow());
    }

    @Test
    public void shouldGetPersistManager() {
        assertNotNull(template.getEventManager());
        assertEquals(persistManager, template.getEventManager());
    }


    @Test
    public void shouldGetClassMappings() {
        assertNotNull(template.getEntities());
        assertEquals(entities, template.getEntities());
    }

    @Test
    public void shouldGetConverters() {
        assertNotNull(template.getConverters());
        assertEquals(converters, template.getConverters());
    }
}