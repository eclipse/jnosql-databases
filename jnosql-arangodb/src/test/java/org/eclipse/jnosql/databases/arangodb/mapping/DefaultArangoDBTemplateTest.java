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
package org.eclipse.jnosql.databases.arangodb.mapping;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.databases.arangodb.communication.ArangoDBDocumentManager;
import org.eclipse.jnosql.databases.arangodb.communication.Person;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Converters.class, DocumentEntityConverter.class, AQL.class})
@AddPackages(MockProducer.class)
@AddExtensions({EntityMetadataExtension.class, DocumentExtension.class, ArangoDBExtension.class})
@ExtendWith(MockitoExtension.class)
@AddPackages(Reflections.class)
public class DefaultArangoDBTemplateTest {

    @Inject
    private DocumentEntityConverter converter;

    @Inject
    private DocumentEventPersistManager persistManager;

    @Inject
    private EntitiesMetadata entities;
    @Inject
    private Converters converters;

    private ArangoDBDocumentManager manager;

    private ArangoDBTemplate template;


    @BeforeEach
    public void setup() {
        manager = Mockito.mock(ArangoDBDocumentManager.class);
        Instance instance = Mockito.mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultArangoDBTemplate(instance, converter, persistManager, entities, converters);

        DocumentEntity entity = DocumentEntity.of("Person");
        entity.add(Document.of("_id", "Ada"));
        entity.add(Document.of("age", 10));

    }

    @Test
    public void shouldFindAQL() {
        Map<String, Object> params = Collections.singletonMap("name", "Ada");
        template.aql("FOR p IN Person FILTER p.name = @name RETURN p", params);
        Mockito.verify(manager).aql("FOR p IN Person FILTER p.name = @name RETURN p", params);
    }

    @Test
    public void shouldFindAQLWithTypeAndParameters() {
        Map<String, Object> params = Collections.singletonMap("name", "Ada");
        template.aql("FOR p IN Person FILTER p.name = @name RETURN p", params, String.class);
        Mockito.verify(manager).aql("FOR p IN Person FILTER p.name = @name RETURN p", params, String.class);
    }

    @Test
    public void shouldFindAQLWithType() {
        template.aql("FOR p IN Person FILTER p.name = @name RETURN p", String.class);
        Mockito.verify(manager).aql("FOR p IN Person FILTER p.name = @name RETURN p", String.class);
    }

    @Test
    public void shouldDeleteAll(){
        ArgumentCaptor<DocumentDeleteQuery> argumentCaptor = ArgumentCaptor.forClass(DocumentDeleteQuery.class);
        template.deleteAll(Person.class);
        Mockito.verify(manager).delete(argumentCaptor.capture());
        DocumentDeleteQuery query = argumentCaptor.getValue();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(query.name()).isEqualTo("Person");
            soft.assertThat(query.documents()).isEmpty();
            soft.assertThat(query.condition()).isEmpty();
        });

    }


}
