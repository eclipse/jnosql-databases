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
package org.eclipse.jnosql.databases.hazelcast.mapping;

import jakarta.data.repository.PageableRepository;
import jakarta.nosql.keyvalue.KeyValueTemplate;
import org.eclipse.jnosql.mapping.keyvalue.query.AbstractKeyValueRepository;
import org.eclipse.jnosql.mapping.keyvalue.query.AbstractKeyValueRepositoryProxy;
import org.eclipse.jnosql.mapping.metadata.EntityMetadata;
import org.eclipse.jnosql.mapping.repository.DynamicReturn;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.eclipse.jnosql.mapping.repository.DynamicReturn.toSingleResult;

class HazelcastRepositoryProxy<T> extends AbstractKeyValueRepositoryProxy<T> {

    private final HazelcastTemplate template;

    private final PageableRepository<?, ?> repository;

    private final Class<T> typeClass;

    private final Class<?> repositoryType;


    HazelcastRepositoryProxy(HazelcastTemplate template, Class<?> repositoryType, PageableRepository<?, ?> repository) {
        this.template = template;
        this.typeClass = Class.class.cast(ParameterizedType.class.cast(repositoryType.getGenericInterfaces()[0])
                .getActualTypeArguments()[0]);
        this.repository = repository;
        this.repositoryType = repositoryType;
    }

    @Override
    protected PageableRepository getRepository() {
        return repository;
    }

    @Override
    protected KeyValueTemplate getTemplate() {
        return template;
    }

    @Override
    protected Class<T> getType() {
        return typeClass;
    }

    @Override
    protected Class<?> repositoryType() {
        return repositoryType;
    }

    @Override
    public Object invoke(Object instance, Method method, Object[] args) throws Throwable {

        Query query = method.getAnnotation(Query.class);
        if (Objects.nonNull(query)) {
            Stream<T> result;
            Map<String, Object> params = ParamUtil.getParams(args, method);
            if (params.isEmpty()) {
                result = template.<T>sql(query.value()).stream();
            } else {
                result = template.<T>sql(query.value(), params).stream();
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


}
