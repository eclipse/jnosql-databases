package org.jnosql.diana.dynamodb.key;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DynamoDBKeyValueConfigurationTest {
	
	private DynamoDBKeyValueConfiguration configuration;
	
	@BeforeEach
	public void setUp(){
		configuration = new DynamoDBKeyValueConfiguration();
	}

	@Test
	public void shouldCreateKeyValueFactoryFromFile() {
		BucketManagerFactory managerFactory = configuration.get();
		assertNotNull(managerFactory);
	}
}
