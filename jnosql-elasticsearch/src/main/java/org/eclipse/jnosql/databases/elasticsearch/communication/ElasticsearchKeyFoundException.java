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
package org.eclipse.jnosql.databases.elasticsearch.communication;

/**
 * An exception when {@link ElasticsearchDocumentManager} tries to both update and insert, but it does not found
 * The column with the name "_id".
 */
public class ElasticsearchKeyFoundException extends RuntimeException {

    ElasticsearchKeyFoundException(String message) {
        super("The entity was not found at: " + message);
    }
}
