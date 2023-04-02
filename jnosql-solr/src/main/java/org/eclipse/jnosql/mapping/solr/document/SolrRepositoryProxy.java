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
package org.eclipse.jnosql.mapping.solr.document;


import jakarta.data.repository.PageableRepository;
import org.eclipse.jnosql.mapping.repository.DynamicReturn;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.eclipse.jnosql.mapping.repository.DynamicReturn.toSingleResult;

class SolrRepositoryProxy<T> implements InvocationHandler {

    private final Class<T> typeClass;

    private final SolrTemplate template;

    private final PageableRepository<?, ?> repository;


    SolrRepositoryProxy(SolrTemplate template, Class<?> repositoryType, PageableRepository<?, ?> repository) {
        this.template = template;
        this.typeClass = Class.class.cast(ParameterizedType.class.cast(repositoryType.getGenericInterfaces()[0])
                .getActualTypeArguments()[0]);
        this.repository = repository;
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
        return method.invoke(repository, args);
    }


}
