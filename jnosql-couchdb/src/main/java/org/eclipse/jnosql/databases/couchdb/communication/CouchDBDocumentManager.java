/*
 *
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
 *
 */
package org.eclipse.jnosql.databases.couchdb.communication;

import org.eclipse.jnosql.communication.document.DocumentManager;

/**
 * A couchdb extension where it does provide a {@link CouchDBDocumentManager#count()} feature.
 */
public interface CouchDBDocumentManager extends DocumentManager {

    /**
     * Returns the number of elements of database
     *
     * @return the number of elements
     * @throws UnsupportedOperationException when the database dot not have support
     */
    long count();
}
