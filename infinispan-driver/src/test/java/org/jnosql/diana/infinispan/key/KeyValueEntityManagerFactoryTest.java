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
 *   The Infinispan Team
 */
package org.jnosql.diana.infinispan.key;

import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.junit.Before;
import org.junit.Test;


public class KeyValueEntityManagerFactoryTest {

    public static final String BUCKET_NAME = "bucketName";
    private BucketManagerFactory managerFactory;

    @Before
    public void setUp() {
        KeyValueConfiguration configuration = new InfinispanKeyValueConfiguration();
        managerFactory = configuration.get();
    }

    @Test
    public void shouldCreateKeyValueEntityManager(){
        BucketManager keyValueEntityManager = managerFactory.getBucketManager(BUCKET_NAME);
        assertNotNull(keyValueEntityManager);
    }

    @Test
    public void shouldCreateMap(){
        Map<String, String> map = managerFactory.getMap(BUCKET_NAME, String.class, String.class);
        assertNotNull(map);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldCreateSet(){
        managerFactory.getSet(BUCKET_NAME, String.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldCreateList(){
        managerFactory.getList(BUCKET_NAME, String.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldCreateQueue(){
        managerFactory.getQueue(BUCKET_NAME, String.class);
    }

}
