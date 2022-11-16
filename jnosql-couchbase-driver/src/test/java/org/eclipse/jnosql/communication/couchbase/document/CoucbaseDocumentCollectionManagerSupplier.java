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
package org.eclipse.jnosql.communication.couchbase.document;

import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentManager;
import jakarta.nosql.tck.communication.driver.document.DocumentCollectionManagerSupplier;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.eclipse.jnosql.communication.couchbase.DatabaseContainer;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.Map;

public class CoucbaseDocumentCollectionManagerSupplier implements DocumentCollectionManagerSupplier {


    @Override
    public DocumentManager get() {
        CouchbaseDocumentConfiguration configuration = DatabaseContainer.INSTANCE.getDocumentConfiguration();
        Settings settings = CouchbaseUtil.getSettings();
        CouchbaseDocumentManagerFactory managerFactory = configuration.apply(settings);
        return managerFactory.apply(CouchbaseUtil.BUCKET_NAME);
    }

}
