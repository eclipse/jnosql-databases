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

import org.jnosql.diana.api.Settings;

import static java.util.Optional.ofNullable;

final class DynamoDBBuilders {


    private static final String ENDPOINT = "dynamodb.endpoint";
    private static final String REGION = "dynamodb.region";
    private static final String PROFILE = "dynamodb.profile";
    private static final String AWS_ACCESSKEY = "dynamodb.awsaccesskey";
    private static final String AWS_SECRET_ACCESS = "dynamodb.secretaccess";

    private DynamoDBBuilders() {
    }

    static void load(Settings settings, DynamoDBBuilder dynamoDB) {
        ofNullable(settings.get(ENDPOINT)).map(Object::toString).ifPresent(dynamoDB::endpoint);
        ofNullable(settings.get(REGION)).map(Object::toString).ifPresent(dynamoDB::region);
        ofNullable(settings.get(PROFILE)).map(Object::toString).ifPresent(dynamoDB::profile);
        ofNullable(settings.get(AWS_ACCESSKEY)).map(Object::toString).ifPresent(dynamoDB::awsAccessKey);
        ofNullable(settings.get(AWS_SECRET_ACCESS)).map(Object::toString).ifPresent(dynamoDB::awsSecretAccess);
    }

}
