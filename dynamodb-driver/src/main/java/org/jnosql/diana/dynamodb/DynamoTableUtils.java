/*
 *  Copyright (c) 2018 Otávio Santana and others
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
package org.jnosql.diana.dynamodb;

import static org.jnosql.diana.api.key.KeyValueEntity.KEY;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class DynamoTableUtils {

	public static KeySchemaElement createKeyElementSchema(Map<String,KeyType> keys) {
		
		KeySchemaElement.Builder keySchemaElementBuilder = KeySchemaElement.builder();
		
		keys
		.entrySet()
		.forEach(
			es -> {
				keySchemaElementBuilder.attributeName(es.getKey());
				keySchemaElementBuilder.keyType(es.getValue());
			});
		
		return keySchemaElementBuilder.build();
	}
	
	public static AttributeDefinition createAttributeDefinition(Map<String,ScalarAttributeType> attributes) {
		
		AttributeDefinition.Builder attributeDefinitionBuilder = AttributeDefinition.builder();

		attributes
		.entrySet()
		.forEach(
			es ->{
				attributeDefinitionBuilder.attributeName(es.getKey());
				attributeDefinitionBuilder.attributeType(es.getValue());
			}
		 );
		
		return attributeDefinitionBuilder.build();
	}
	
	public static ProvisionedThroughput createProvisionedThroughput(Long readCapacityUnits , Long writeCapacityUnit) {
		
		ProvisionedThroughput.Builder provisionedThroughputBuilder = ProvisionedThroughput.builder();
		
		if(readCapacityUnits != null && readCapacityUnits.longValue() > 0)
			provisionedThroughputBuilder.readCapacityUnits(readCapacityUnits);
		else
			provisionedThroughputBuilder.readCapacityUnits(5l);
		
		
		if(writeCapacityUnit != null && writeCapacityUnit.longValue() > 0)
			provisionedThroughputBuilder.writeCapacityUnits(writeCapacityUnit);
		else
			provisionedThroughputBuilder.writeCapacityUnits(5l);
		
		return provisionedThroughputBuilder.build();
	}
	
	public static Map<String, KeyType> createKeyDefinition(){
		return Collections.singletonMap(KEY,KeyType.HASH);
	}
	
	public static Map<String, ScalarAttributeType> createAttributesType(){
		return Collections.singletonMap(KEY, ScalarAttributeType.S);
	}

	public static void manageTables(String tableName, DynamoDbClient client, Long readCapacityUnits , Long writeCapacityUnit) {

		boolean more_tables = true;
		String last_name = null;

		while (more_tables) {
			try {
				ListTablesResponse response = null;
				if (last_name == null) {
					ListTablesRequest request = ListTablesRequest.builder().build();
					response = client.listTables(request);
				} else {
					ListTablesRequest request = ListTablesRequest.builder().exclusiveStartTableName(last_name).build();
					response = client.listTables(request);
				}

				List<String> table_names = response.tableNames();

				if (table_names.size() == 0) {
					createTable(tableName, client,readCapacityUnits ,writeCapacityUnit);
				} else {
					last_name = response.lastEvaluatedTableName();
					if (last_name == null) {
						more_tables = false;
					}
				}
			} catch (DynamoDbException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static void createTable(String tableName, DynamoDbClient client , Long readCapacityUnits , Long writeCapacityUnit) {
		
		Map<String, KeyType> keyDefinition = createKeyDefinition();
		Map<String, ScalarAttributeType> attributeDefinition = createAttributesType();
		
		client.createTable(CreateTableRequest.builder()
				.tableName(tableName)
				.provisionedThroughput(createProvisionedThroughput(readCapacityUnits, writeCapacityUnit))
				.keySchema(createKeyElementSchema(keyDefinition))
				.attributeDefinitions(createAttributeDefinition(attributeDefinition))
				.build());
	}
}
