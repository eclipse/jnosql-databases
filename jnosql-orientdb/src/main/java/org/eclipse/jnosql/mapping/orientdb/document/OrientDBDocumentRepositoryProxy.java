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
package org.eclipse.jnosql.mapping.orientdb.document;


import jakarta.data.repository.PageableRepository;
import org.eclipse.jnosql.mapping.repository.DynamicReturn;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.eclipse.jnosql.mapping.repository.DynamicReturn.toSingleResult;


class OrientDBDocumentRepositoryProxy<T> implements InvocationHandler {

    private final Class<T> typeClass;

    private final OrientDBTemplate template;

    private final PageableRepository<?, ?> repository;


    OrientDBDocumentRepositoryProxy(OrientDBTemplate template, Class<?> repositoryType, PageableRepository<?, ?> repository) {
        this.template = template;
        this.typeClass = Class.class.cast(ParameterizedType.class.cast(repositoryType.getGenericInterfaces()[0])
                .getActualTypeArguments()[0]);
        this.repository = repository;
    }


    @Override
    public Object invoke(Object instance, Method method, Object[] args) throws Throwable {

        SQL sql = method.getAnnotation(SQL.class);
        if (Objects.nonNull(sql)) {
            Stream<T> result;

            if (args == null || args.length == 0) {
                result = template.sql(sql.value());
            } else {
                Map<String, Object> params = MapTypeUtil.getParams(args, method);
                if (params.isEmpty()) {
                    result = template.sql(sql.value(), args);
                } else {
                    result = template.sql(sql.value(), params);
                }
            }
            return DynamicReturn.builder()
                    .withClassSource(typeClass)
                    .withMethodSource(method).withResult(() -> result)
                    .withSingleResult(toSingleResult(method).apply(() -> result))
                    .build().execute();
        }
        return method.invoke(repository, args);
    }


}
