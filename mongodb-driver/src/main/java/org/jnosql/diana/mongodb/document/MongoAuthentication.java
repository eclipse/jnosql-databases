/*
 *  Copyright (c) 2019 Otávio Santana and others
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

package org.jnosql.diana.mongodb.document;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCredential;
import org.jnosql.diana.api.Configurations;
import org.jnosql.diana.api.JNoSQLException;
import org.jnosql.diana.api.Settings;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_MECHANISM;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_SOURCE;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.PASSWORD;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.USER;

final class MongoAuthentication {

    private MongoAuthentication() {
    }

    static Optional<MongoCredential> of(Settings settings) {


        Optional<String> user = settings.get(Arrays.asList(USER.get(),
                Configurations.USER.get()))
                .map(Object::toString);

        Optional<char[]> password = settings.get(Arrays.asList(PASSWORD.get(),
                Configurations.PASSWORD.get()))
                .map(Object::toString).map(String::toCharArray);

        Optional<String> source = settings.get(AUTHENTICATION_SOURCE.get())
                .map(Object::toString);

        AuthenticationMechanism mechanism = settings.get(AUTHENTICATION_MECHANISM.get())
                .map(Object::toString)
                .map(AuthenticationMechanism::fromMechanismName)
                .orElse(AuthenticationMechanism.PLAIN);

        if (!user.isPresent()) {
            return Optional.empty();
        }

        switch (mechanism) {
            case PLAIN:
                return Optional.of(MongoCredential.createPlainCredential(user.orElseThrow(missingExceptionUser()),
                        source.orElseThrow(missingExceptionSource()), password.orElseThrow(missingExceptionPassword())));
            case GSSAPI:
                return Optional.of(MongoCredential.createGSSAPICredential(user.orElseThrow(missingExceptionUser())));
            case SCRAM_SHA_1:
                return Optional.of(MongoCredential.createScramSha1Credential(user.orElseThrow(missingExceptionUser()),
                        source.orElseThrow(missingExceptionSource()), password.orElseThrow(missingExceptionPassword())));
            case MONGODB_X509:
                return Optional.of(MongoCredential.createMongoX509Credential(user.orElseThrow(missingExceptionUser())));
            case SCRAM_SHA_256:
                return Optional.of(MongoCredential.createScramSha256Credential(user.orElseThrow(missingExceptionUser()),
                        source.orElseThrow(missingExceptionSource()), password.orElseThrow(missingExceptionPassword())));
            default:
                throw new JNoSQLException("There is not support to the type: " + mechanism);
        }

    }


    private static Supplier<JNoSQLException> missingExceptionUser() {
        return missingException("user");
    }

    private static Supplier<JNoSQLException> missingExceptionPassword() {
        return missingException("password");
    }

    private static Supplier<JNoSQLException> missingExceptionSource() {
        return missingException("source");
    }


    private static Supplier<JNoSQLException> missingException(String parameter) {
        return () -> new JNoSQLException("There is a missing parameter in mongoDb authentication: " + parameter);
    }


}
