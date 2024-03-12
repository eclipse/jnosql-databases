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

import jakarta.data.repository.Param;
import jakarta.inject.Inject;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.query.AbstractRepository;
import org.eclipse.jnosql.mapping.core.repository.DynamicReturn;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.metadata.EntityMetadata;
import org.eclipse.jnosql.mapping.semistructured.SemistructuredTemplate;
import org.eclipse.jnosql.mapping.semistructured.query.AbstractSemistructuredRepositoryProxy;
import org.eclipse.jnosql.mapping.semistructured.query.SemistructuredRepositoryProxy;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.eclipse.jnosql.mapping.core.repository.DynamicReturn.toSingleResult;

class DynamoDBRepositoryProxy<T, K> extends AbstractSemistructuredRepositoryProxy<T, K> {

    private final DynamoDBTemplate template;

    private final Class<?> type;

    private final Converters converters;

    private final Class<T> typeClass;

    private final EntityMetadata entityMetadata;

    private final AbstractRepository<?, ?> repository;

    @Inject
    @SuppressWarnings("unchecked")
    DynamoDBRepositoryProxy(DynamoDBTemplate template,
                            Class<?> type,
                            Converters converters,
                            EntitiesMetadata entitiesMetadata) {

        this.template = template;
        this.type = type;
        this.typeClass = (Class<T>) ((ParameterizedType) type.getGenericInterfaces()[0]).getActualTypeArguments()[0];
        this.converters = converters;
        this.entityMetadata = entitiesMetadata.get(typeClass);
        this.repository = SemistructuredRepositoryProxy.SemistructuredRepository.of(template, entityMetadata);
    }

    /**
     * Required by CDI/Reflection/Test purposes
     * Don't use it
     */
    DynamoDBRepositoryProxy() {
        this.template = null;
        this.type = null;
        this.typeClass = null;
        this.converters = null;
        this.entityMetadata = null;
        this.repository = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object instance, Method method, Object[] args) throws Throwable {
        PartiQL sql = method.getAnnotation(PartiQL.class);
        if (Objects.nonNull(sql)) {
            Stream<T> result;
            List<Object> params = getParams(args, method);
            if (params.isEmpty()) {
                result = template.partiQL(sql.value());
            } else {
                result = template.partiQL(sql.value(), params.toArray());
            }
            return DynamicReturn.builder()
                    .withClassSource(typeClass)
                    .withMethodSource(method)
                    .withResult(() -> result)
                    .withSingleResult(toSingleResult(method).apply(() -> result))
                    .build().execute();
        }
        return super.invoke(instance, method, args);
    }

    private List<Object> getParams(Object[] args, Method method) {

        List<Object> params = new ArrayList<>();
        Annotation[][] annotations = method.getParameterAnnotations();

        for (int index = 0; index < annotations.length; index++) {

            final Object arg = args[index];

            Optional<Param> param = Stream.of(annotations[index])
                    .filter(Param.class::isInstance)
                    .map(Param.class::cast)
                    .findFirst();
            param.ifPresent(p -> params.add(arg));

        }

        return params;
    }

    @Override
    protected Converters converters() {
        return converters;
    }

    @Override
    @SuppressWarnings({"rawtypes","unchecked"})
    protected AbstractRepository repository() {
        return repository;
    }

    @Override
    protected Class<?> repositoryType() {
        return type;
    }

    @Override
    protected EntityMetadata entityMetadata() {
        return entityMetadata;
    }

    @Override
    protected SemistructuredTemplate template() {
        return template;
    }
}
