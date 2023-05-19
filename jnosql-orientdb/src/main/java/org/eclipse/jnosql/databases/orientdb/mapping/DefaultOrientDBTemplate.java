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


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBDocumentManager;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBLiveCallback;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBLiveCallbackBuilder;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.AbstractDocumentTemplate;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.DocumentEventPersistManager;
import org.eclipse.jnosql.mapping.document.DocumentWorkflow;
import org.eclipse.jnosql.mapping.reflection.EntitiesMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * The Default implementation of {@link OrientDBTemplate}
 */
@Typed(OrientDBTemplate.class)
@ApplicationScoped
class DefaultOrientDBTemplate extends AbstractDocumentTemplate
        implements OrientDBTemplate {

    private final Instance<OrientDBDocumentManager> manager;

    private final DocumentEntityConverter converter;

    private final DocumentWorkflow flow;

    private final DocumentEventPersistManager persistManager;

    private final EntitiesMetadata entities;

    private final Converters converters;

    @Inject
    DefaultOrientDBTemplate(Instance<OrientDBDocumentManager> manager,
                            DocumentEntityConverter converter, DocumentWorkflow flow,
                            DocumentEventPersistManager persistManager,
                            EntitiesMetadata entities,
                            Converters converters) {

        this.manager = manager;
        this.converter = converter;
        this.flow = flow;
        this.persistManager = persistManager;
        this.entities = entities;
        this.converters = converters;
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
    protected DocumentWorkflow getWorkflow() {
        return flow;
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
    public <T> Stream<T> sql(String query, Object... params) {
        return manager.get().sql(query, params).map(converter::toEntity)
                .map(e -> (T) e);
    }

    @Override
    public <T> Stream<T> sql(String query, Map<String, Object> params) {
        return manager.get().sql(query, params).map(converter::toEntity)
                .map(e -> (T) e);
    }

    @Override
    public <T> void live(DocumentQuery query, OrientDBLiveCallback<T> callBacks) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(callBacks, "callBacks is required");
        manager.get().live(query, bindCallbacks(callBacks));
    }

    @Override
    public <T> void live(String query, OrientDBLiveCallback<T> callBacks, Object... params) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(callBacks, "callBack is required");
        manager.get().live(query, bindCallbacks(callBacks), params);
    }

    private <T> OrientDBLiveCallback<DocumentEntity> bindCallbacks(OrientDBLiveCallback<T> callBacks) {
        return OrientDBLiveCallbackBuilder.builder()
                .onCreate(d -> callBacks.getCreateCallback().ifPresent(callback -> callback.accept(converter.toEntity(d))))
                .onUpdate(d -> callBacks.getUpdateCallback().ifPresent(callback -> callback.accept(converter.toEntity(d))))
                .onDelete(d -> callBacks.getDeleteCallback().ifPresent(callback -> callback.accept(converter.toEntity(d))))
                .build();
    }
}
