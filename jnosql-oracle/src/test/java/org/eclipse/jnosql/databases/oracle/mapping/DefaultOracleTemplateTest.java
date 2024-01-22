/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.mapping;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.databases.oracle.communication.OracleDocumentManager;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.Reflections;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@EnableAutoWeld
@AddPackages(value = {Converters.class, DocumentEntityConverter.class, SQL.class})
@AddPackages(MockProducer.class)
@AddExtensions({EntityMetadataExtension.class, DocumentExtension.class, OracleExtension.class})
@ExtendWith(MockitoExtension.class)
@AddPackages(Reflections.class)
class DefaultOracleTemplateTest {


    @Inject
    private DocumentEntityConverter converter;

    @Inject
    private DocumentEventPersistManager persistManager;

    @Inject
    private EntitiesMetadata entities;
    @Inject
    private Converters converters;

    private OracleDocumentManager manager;

    private OracleTemplate template;

    @BeforeEach
    public void setup() {
        manager = Mockito.mock(OracleDocumentManager.class);
        Instance instance = Mockito.mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultOracleTemplate(instance, converter, persistManager, entities, converters);

        DocumentEntity entity = DocumentEntity.of("Person");
        entity.add(Document.of("_id", "Ada"));
        entity.add(Document.of("age", 10));

    }

    @Test
    public void shouldFindSQL() {
        template.sql("select from database");
        Mockito.verify(manager).sql("select from database");
    }

    @Test
    public void shouldFindSQLWithTypeAndParameters() {
        template.sql("select from database where content.name = ?", List.of("Ada"), String.class);
        Mockito.verify(manager).sql("select from database where content.name = ?", List.of("Ada"), String.class);
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