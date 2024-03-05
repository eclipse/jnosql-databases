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
package org.eclipse.jnosql.databases.arangodb.mapping;


import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.interceptor.Interceptor;
import org.eclipse.jnosql.communication.semistructured.CommunicationEntity;
import org.eclipse.jnosql.communication.semistructured.Element;
import org.eclipse.jnosql.databases.arangodb.communication.ArangoDBDocumentManager;
import org.mockito.Mockito;

import java.util.function.Supplier;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class MockProducer implements Supplier<ArangoDBDocumentManager> {

    @Produces
    @Override
    public ArangoDBDocumentManager get() {
        ArangoDBDocumentManager manager = Mockito.mock(ArangoDBDocumentManager.class);
        CommunicationEntity entity = CommunicationEntity.of("Person");
        entity.add(Element.of("name", "Ada"));
        Mockito.when(manager.insert(Mockito.any(CommunicationEntity.class))).thenReturn(entity);
        return manager;
    }

}
