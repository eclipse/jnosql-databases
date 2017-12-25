/*
 *  Copyright (c) 2017 Otávio Santana and others
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
package org.jnosql.diana.couchbase.key;

import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.couchbase.CouchbaseUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class DefaultCouchbaseBucketManagerFactoryTest {

    private CouchbaseBucketManagerFactory factory;

    @Before
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        factory = configuration.get();
    }

    @Test
    public void shouldReturnManager() {
        BucketManager database = factory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        Assert.assertNotNull(database);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnError() {
        factory.getBucketManager(null);
    }
}