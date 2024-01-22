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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.databases.oracle.communication.OracleDocumentManager;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.document.AbstractDocumentTemplate;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

@Typed(OracleTemplate.class)
@ApplicationScoped
class DefaultOracleTemplate extends AbstractDocumentTemplate implements OracleTemplate {

    private Instance<OracleDocumentManager> manager;

    private DocumentEntityConverter converter;

    private DocumentEventPersistManager persistManager;

    private EntitiesMetadata entities;

    private Converters converters;

    @Inject
    DefaultOracleTemplate(Instance<OracleDocumentManager> manager,
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

    DefaultOracleTemplate() {
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
    public <T> Stream<T> sql(String query) {
        Objects.requireNonNull(query, "query is required");
        return manager.get().sql(query).map(converter::toEntity).map(d -> (T) d);
    }

    @Override
    public <T> Stream<T> sql(String query, Object... params) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(params, "params is required");
        return manager.get().sql(query, params).map(converter::toEntity).map(d -> (T) d);
    }
}
