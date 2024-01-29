/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.mapping;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBDocumentManager;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.document.AbstractDocumentTemplate;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;

import java.util.Objects;
import java.util.stream.Stream;

@Typed(DynamoDBTemplate.class)
@ApplicationScoped
class DefaultDynamoDBTemplate extends AbstractDocumentTemplate implements DynamoDBTemplate {

    private Instance<DynamoDBDocumentManager> manager;

    private DocumentEntityConverter converter;

    private DocumentEventPersistManager persistManager;

    private EntitiesMetadata entitiesMetadata;

    private Converters converters;

    @Inject
    DefaultDynamoDBTemplate(Instance<DynamoDBDocumentManager> manager,
                            DocumentEntityConverter converter,
                            DocumentEventPersistManager persistManager,
                            EntitiesMetadata entitiesMetadata,
                            Converters converters) {
        this.manager = manager;
        this.converter = converter;
        this.persistManager = persistManager;
        this.entitiesMetadata = entitiesMetadata;
        this.converters = converters;
    }

    /**
     * Required by CDI/Reflection/Test purposes
     * Don't use it
     */
    DefaultDynamoDBTemplate() {
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
        return entitiesMetadata;
    }

    @Override
    protected Converters getConverters() {
        return converters;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Stream<T> partiQL(String query) {
        Objects.requireNonNull(query, "query is required");
        return manager.get().partiQL(query).map(converter::toEntity).map(d -> (T) d);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Stream<T> partiQL(String query, Object... params) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(params, "params is required");
        return manager.get().partiQL(query, params).map(converter::toEntity).map(d -> (T) d);
    }
}
