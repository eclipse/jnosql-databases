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

import jakarta.data.repository.PageableRepository;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.util.AnnotationLiteral;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.query.DocumentRepositoryProducer;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.spi.AbstractBean;

import java.lang.annotation.Annotation;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Set;


class OrientDBRepositoryBean extends AbstractBean<OrientDBCrudRepository> {

    private final Class type;

    private final Set<Type> types;

    private final Set<Annotation> qualifiers = Collections.singleton(new AnnotationLiteral<Default>() {
    });

    OrientDBRepositoryBean(Class type) {
        this.type = type;
        this.types = Collections.singleton(type);
    }

    @Override
    public Class<?> getBeanClass() {
        return type;
    }

    @Override
    public OrientDBCrudRepository create(CreationalContext<OrientDBCrudRepository> creationalContext) {
        OrientDBTemplate template = getInstance(OrientDBTemplate.class);
        DocumentRepositoryProducer producer = getInstance(DocumentRepositoryProducer.class);
        PageableRepository<?, ?> repository = producer.get((Class<PageableRepository<Object, Object>>) type, template);
        Converters converters = getInstance(Converters.class);
        EntitiesMetadata entitiesMetadata = getInstance(EntitiesMetadata.class);
        OrientDBDocumentRepositoryProxy handler = new OrientDBDocumentRepositoryProxy(template, type, repository,
                converters, entitiesMetadata);
        return (OrientDBCrudRepository) Proxy.newProxyInstance(type.getClassLoader(),
                new Class[]{type},
                handler);
    }

    @Override
    public Set<Type> getTypes() {
        return types;
    }

    @Override
    public Set<Annotation> getQualifiers() {
        return qualifiers;
    }

    @Override
    public boolean isAlternative() {
        return false;
    }

    @Override
    public String getId() {
        return type.getName() + "@orientdb";
    }

}