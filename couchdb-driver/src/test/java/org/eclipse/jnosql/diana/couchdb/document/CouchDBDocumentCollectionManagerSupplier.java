/*
 *  Copyright (c) 2020 Otávio Santana and others
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
package org.eclipse.jnosql.diana.couchdb.document;

import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.tck.communication.driver.document.DocumentCollectionManagerSupplier;
import org.eclipse.jnosql.diana.couchdb.document.configuration.CouchDBDocumentTcConfiguration;

public class CouchDBDocumentCollectionManagerSupplier implements DocumentCollectionManagerSupplier {

    private static final String DATABASE = "tck-database";

    @Override
    public DocumentCollectionManager get() {
        final CouchDBDocumentCollectionManagerFactory factory = CouchDBDocumentTcConfiguration.INSTANCE.get();
        return factory.get(DATABASE);
    }

}
