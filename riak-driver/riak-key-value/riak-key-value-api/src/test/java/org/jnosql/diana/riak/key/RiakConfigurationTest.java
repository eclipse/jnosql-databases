package org.jnosql.diana.riak.key;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;


public class RiakConfigurationTest {

    private RiakConfiguration configuration;

    @Before
    public void setUp() {
        configuration = new RiakConfiguration();
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErroWhenNodeIsNull() {
        configuration.add(null);
    }

    @Test
    public void shouldCreateKeyValueFactoryFromFile() {
        BucketManagerFactory managerFactory = configuration.getManagerFactory();
        assertNotNull(managerFactory);
    }
}