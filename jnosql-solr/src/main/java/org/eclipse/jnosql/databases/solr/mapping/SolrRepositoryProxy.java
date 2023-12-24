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


import jakarta.data.repository.PageableRepository;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.document.JNoSQLDocumentTemplate;
import org.eclipse.jnosql.mapping.document.query.AbstractDocumentRepositoryProxy;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.metadata.EntityMetadata;
import org.eclipse.jnosql.mapping.core.repository.DynamicReturn;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.eclipse.jnosql.mapping.core.repository.DynamicReturn.toSingleResult;

class SolrRepositoryProxy<T, K> extends AbstractDocumentRepositoryProxy<T, K> {

    private final Class<T> typeClass;

    private final SolrTemplate template;

    private final PageableRepository<?, ?> repository;

    private final Class<?> repositoryType;

    private final Converters converters;

    private final EntityMetadata entityMetadata;


    SolrRepositoryProxy(SolrTemplate template, Class<?> repositoryType, PageableRepository<?, ?> repository,
                        Converters converters,
                        EntitiesMetadata entitiesMetadata) {
        this.template = template;
        this.typeClass = Class.class.cast(ParameterizedType.class.cast(repositoryType.getGenericInterfaces()[0])
                .getActualTypeArguments()[0]);
        this.repository = repository;
        this.converters = converters;
        this.repositoryType = repositoryType;
        this.entityMetadata = entitiesMetadata.get(typeClass);
    }

    @Override
    protected PageableRepository getRepository() {
        return repository;
    }

    @Override
    protected Class<?> repositoryType() {
        return repositoryType;
    }

    @Override
    protected Converters getConverters() {
        return converters;
    }

    @Override
    protected EntityMetadata getEntityMetadata() {
        return entityMetadata;
    }

    @Override
    protected JNoSQLDocumentTemplate getTemplate() {
        return template;
    }

    @Override
    public Object invoke(Object instance, Method method, Object[] args) throws Throwable {

        Solr solr = method.getAnnotation(Solr.class);
        if (Objects.nonNull(solr)) {
            List<T> result;
            Map<String, Object> params = MapParams.getParams(args, method);
            if (params.isEmpty()) {
                result = template.solr(solr.value());
            } else {
                result = template.solr(solr.value(), params);
            }

            return DynamicReturn.builder()
                    .withClassSource(typeClass)
                    .withMethodSource(method)
                    .withResult(result::stream)
                    .withSingleResult(toSingleResult(method).apply(result::stream))
                    .build().execute();
        }
        return super.invoke(instance, method, args);
    }



}
