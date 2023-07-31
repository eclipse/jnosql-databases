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
package org.eclipse.jnosql.databases.solr.mapping;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.databases.solr.communication.SolrDocumentManager;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Convert.class,
        DocumentEntityConverter.class, Solr.class})
@AddPackages(MockProducer.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class, SolrExtension.class})
public class DefaultSolrTemplateTest {

    @Inject
    private DocumentEntityConverter converter;

    @Inject
    private DocumentEventPersistManager persistManager;

    @Inject
    private EntitiesMetadata entities;

    @Inject
    private Converters converters;

    private SolrDocumentManager manager;

    private SolrTemplate template;


    @BeforeEach
    public void setup() {
        manager = Mockito.mock(SolrDocumentManager.class);
        Instance instance = Mockito.mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultSolrTemplate(instance, converter, persistManager, entities, converters);

        DocumentEntity entity = DocumentEntity.of("Person");
        entity.add(Document.of("_id", "Ada"));
        entity.add(Document.of("age", 10));
    }

    @Test
    public void shouldFindBySolr() {
        Map<String, String> params = Collections.singletonMap("name", "ada");
        template.solr("name:@name _entity:person", params);
        Mockito.verify(manager).solr("name:@name _entity:person", params);
    }

    @Test
    public void shouldFindBySolr2() {
        template.solr("_entity:person");
        Mockito.verify(manager).solr("_entity:person");
    }


}