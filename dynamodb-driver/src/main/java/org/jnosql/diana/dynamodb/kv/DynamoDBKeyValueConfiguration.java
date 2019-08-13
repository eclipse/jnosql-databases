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
package org.jnosql.diana.dynamodb.kv;

import jakarta.nosql.Settings;
import jakarta.nosql.keyvalue.KeyValueConfiguration;
import org.jnosql.diana.dynamodb.DynamoDBConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDBKeyValueConfiguration extends DynamoDBConfiguration
        implements KeyValueConfiguration {

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
