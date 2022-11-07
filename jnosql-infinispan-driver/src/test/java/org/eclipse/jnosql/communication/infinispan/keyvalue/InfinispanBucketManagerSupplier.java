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
package org.eclipse.jnosql.communication.infinispan.keyvalue;

import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import jakarta.nosql.tck.communication.driver.keyvalue.BucketManagerSupplier;
import org.eclipse.jnosql.communication.infinispan.keyvalue.util.KeyValueEntityManagerFactoryUtils;

public class InfinispanBucketManagerSupplier implements BucketManagerSupplier {

    private static final String BUCKET = "tck-users-entity";

    @Override
    public BucketManager get() {
        final BucketManagerFactory factory = KeyValueEntityManagerFactoryUtils.get();
        return factory.getBucketManager(BUCKET);
    }
}
