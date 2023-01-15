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

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.tck.communication.driver.document.DocumentManagerSupplier;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.eclipse.jnosql.communication.couchbase.DatabaseContainer;

public class CoucbaseDocumentManagerSupplier implements DocumentManagerSupplier {


    @Override
    public DocumentManager get() {
        CouchbaseDocumentConfiguration configuration = DatabaseContainer.INSTANCE.getDocumentConfiguration();
        Settings settings = CouchbaseUtil.getSettings();
        CouchbaseDocumentManagerFactory managerFactory = configuration.apply(settings);
        return managerFactory.apply(CouchbaseUtil.BUCKET_NAME);
    }

}
