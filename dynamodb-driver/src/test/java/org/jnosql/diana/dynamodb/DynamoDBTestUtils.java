package org.jnosql.diana.dynamodb;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.dynamodb.key.DynamoDBKeyValueConfiguration;
import org.testcontainers.containers.GenericContainer;

public class DynamoDBTestUtils {
	
	  private static GenericContainer dynamodb =
	            new GenericContainer("amazon/dynamodb-local:latest")
	                    .withExposedPorts(8000)
	                    .withEnv("AWS_ACCESS_KEY_ID", "aws --profile default configure get aws_access_key_id")
	                    .withEnv("AWS_SECRET_ACCESS_KEY", "aws --profile default configure get aws_secret_access_key");
	                    //.withCommand("--rm");
	  					
	                   // .waitingFor(Wait.forHttp("/")
	                     //       .forStatusCode(200));
	  
	  
	public static BucketManagerFactory get() {
		dynamodb.start();
		DynamoDBKeyValueConfiguration configuration = new DynamoDBKeyValueConfiguration();
		configuration.setEndPoint("http://"+dynamodb.getContainerIpAddress()+":"+dynamodb.getFirstMappedPort());
		return configuration.get();
	}
	
	public static void shutDown() {
		dynamodb.close();
	}
}
