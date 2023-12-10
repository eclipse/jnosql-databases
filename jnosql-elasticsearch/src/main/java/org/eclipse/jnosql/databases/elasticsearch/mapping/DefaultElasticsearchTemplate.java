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
package org.eclipse.jnosql.databases.elasticsearch.mapping;


import co.elastic.clients.elasticsearch.core.SearchRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.databases.elasticsearch.communication.ElasticsearchDocumentManager;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.document.AbstractDocumentTemplate;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * The Default implementation of {@link ElasticsearchTemplate}
 */

@Typed(ElasticsearchTemplate.class)
@ApplicationScoped
class DefaultElasticsearchTemplate extends AbstractDocumentTemplate
        implements ElasticsearchTemplate {

    private Instance<ElasticsearchDocumentManager> manager;

    private DocumentEntityConverter converter;

    private DocumentEventPersistManager persistManager;

    private EntitiesMetadata entities;

    private Converters converters;

    @Inject
    DefaultElasticsearchTemplate(Instance<ElasticsearchDocumentManager> manager,
                                 DocumentEntityConverter converter,
                                 DocumentEventPersistManager persistManager,
                                 EntitiesMetadata entities,
                                 Converters converters) {
        this.manager = manager;
        this.converter = converter;
        this.persistManager = persistManager;
        this.entities = entities;
        this.converters = converters;
    }

    DefaultElasticsearchTemplate() {
    }

    @Override
    protected DocumentEntityConverter getConverter() {
        return converter;
    }

    @Override
    protected DocumentManager getManager() {
        return manager.get();
    }

    @Override
    protected DocumentEventPersistManager getEventManager() {
        return persistManager;
    }

    @Override
    protected EntitiesMetadata getEntities() {
        return entities;
    }

    @Override
    protected Converters getConverters() {
        return converters;
    }

    @Override
    public <T> Stream<T> search(SearchRequest query) {
        Objects.requireNonNull(query, "query is required");
        Stream<DocumentEntity> entities = manager.get().search(query);
        return entities.map(converter::toEntity).map(e -> (T) e);
    }
}
