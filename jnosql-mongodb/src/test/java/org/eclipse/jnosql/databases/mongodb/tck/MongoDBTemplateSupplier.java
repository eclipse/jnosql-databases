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
 *   Alessandro Moscatelli
 */
package org.eclipse.jnosql.databases.mongodb.tck;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.nosql.Template;
import jakarta.nosql.tck.TemplateSupplier;
import org.eclipse.jnosql.databases.mongodb.communication.MongoDBDocumentConfigurations;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;

import static org.eclipse.jnosql.databases.mongodb.communication.DocumentDatabase.INSTANCE;

public class MongoDBTemplateSupplier implements TemplateSupplier {

    static {
        INSTANCE.get("jakarta-nosql-tck");
        System.setProperty(MongoDBDocumentConfigurations.HOST.get() + ".1", INSTANCE.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), "jakarta-nosql-tck");
    }

    @Override
    public Template get() {
        SeContainer container = SeContainerInitializer.newInstance().initialize();
        return container.select(Template.class).get();
    }
}
