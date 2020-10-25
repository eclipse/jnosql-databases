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
package org.eclipse.jnosql.communication.dynamodb;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

public class DynamoDBBuilderSync implements DynamoDBBuilder {


    private final DynamoDbClientBuilder dynamoDB = DynamoDbClient.builder();

    private String awsAccessKey;
    private String awsSecretAccess;


    @Override
    public void endpoint(String endpoint) {
        dynamoDB.endpointOverride(URI.create(endpoint));
    }

    @Override
    public void region(String region) {
        dynamoDB.region(Region.of(region));
    }

    @Override
    public void profile(String profile) {
        dynamoDB.credentialsProvider(ProfileCredentialsProvider.builder()
                .profileName(profile)
                .build());

    }

    public DynamoDbClient build() {

        boolean accessKey = awsAccessKey != null && !awsAccessKey.equals("");
        boolean secretAccess = awsSecretAccess != null && !awsSecretAccess.equals("");


        if (accessKey && secretAccess) {

            AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(awsAccessKey, awsSecretAccess);
            AwsCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
            dynamoDB.credentialsProvider(staticCredentialsProvider);
        }

        return dynamoDB.build();
    }

    @Override
    public void awsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    @Override
    public void awsSecretAccess(String awsSecretAccess) {
        this.awsSecretAccess = awsSecretAccess;
    }
}
