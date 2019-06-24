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

import jakarta.nosql.Settings;

import static java.util.Optional.ofNullable;
import static org.jnosql.diana.dynamodb.DynamoDBConfigurations.AWS_ACCESSKEY;
import static org.jnosql.diana.dynamodb.DynamoDBConfigurations.AWS_SECRET_ACCESS;
import static org.jnosql.diana.dynamodb.DynamoDBConfigurations.ENDPOINT;
import static org.jnosql.diana.dynamodb.DynamoDBConfigurations.PROFILE;
import static org.jnosql.diana.dynamodb.DynamoDBConfigurations.REGION;

final class DynamoDBBuilders {



    private DynamoDBBuilders() {
    }

    static void load(Settings settings, DynamoDBBuilder dynamoDB) {
        ofNullable(settings.get(ENDPOINT.get())).map(Object::toString).ifPresent(dynamoDB::endpoint);
        ofNullable(settings.get(REGION.get())).map(Object::toString).ifPresent(dynamoDB::region);
        ofNullable(settings.get(PROFILE.get())).map(Object::toString).ifPresent(dynamoDB::profile);
        ofNullable(settings.get(AWS_ACCESSKEY.get())).map(Object::toString).ifPresent(dynamoDB::awsAccessKey);
        ofNullable(settings.get(AWS_SECRET_ACCESS.get())).map(Object::toString).ifPresent(dynamoDB::awsSecretAccess);
    }

}
