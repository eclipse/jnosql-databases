package org.jnosql.diana.dynamodb.key;

import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.dynamodb.DynamoDBConfiguration;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDBKeyValueConfiguration extends DynamoDBConfiguration 
	implements KeyValueConfiguration<DynamoDBBucketManagerFactory> {

	@Override
	public DynamoDBBucketManagerFactory get() {
		return new DynamoDBBucketManagerFactory(builder.build());
	}

	@Override
	public DynamoDBBucketManagerFactory get(Settings settings) {
		DynamoDbClient dynamoDB = getDynamoDB(settings);
		return new DynamoDBBucketManagerFactory(dynamoDB);
	}

}
