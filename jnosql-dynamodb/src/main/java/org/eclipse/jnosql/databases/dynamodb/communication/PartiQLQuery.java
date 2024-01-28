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

package org.eclipse.jnosql.databases.dynamodb.communication;

import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.function.Supplier;

public record PartiQLQuery(String query, List<AttributeValue> params, Long limit, Long skip) {
    static Supplier<PartiQLQuery> toPartiQLQuery(String table, String partitionKey, DocumentQuery documentQuery) {
        return new PartiQLQuerySelectBuilder(table, partitionKey, documentQuery);
    }

    static Supplier<PartiQLQuery> toPartiQLQuery(String table, String partitionKey, DocumentDeleteQuery documentDeleteQuery) {
        return new PartiQLQueryDeleteBuilder(table, partitionKey, documentDeleteQuery);
    }

}
