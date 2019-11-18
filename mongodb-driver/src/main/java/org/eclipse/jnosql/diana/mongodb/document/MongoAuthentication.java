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

package org.eclipse.jnosql.diana.mongodb.document;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCredential;
import jakarta.nosql.CommunicationException;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

import static org.eclipse.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_MECHANISM;
import static org.eclipse.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_SOURCE;
import static org.eclipse.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.PASSWORD;
import static org.eclipse.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.USER;

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

        Optional<AuthenticationMechanism> mechanism = settings.get(AUTHENTICATION_MECHANISM.get())
                .map(Object::toString)
                .map(AuthenticationMechanism::fromMechanismName);

        if (!user.isPresent()) {
            return Optional.empty();
        }

        if (!mechanism.isPresent()) {
            return Optional.of(MongoCredential.createCredential(user.orElseThrow(missingExceptionUser()),
                    source.orElseThrow(missingExceptionSource()), password.orElseThrow(missingExceptionPassword())));
        }

        switch (mechanism.get()) {
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
                throw new CommunicationException("There is not support to the type: " + mechanism);
        }

    }


    private static Supplier<CommunicationException> missingExceptionUser() {
        return missingException("user");
    }

    private static Supplier<CommunicationException> missingExceptionPassword() {
        return missingException("password");
    }

    private static Supplier<CommunicationException> missingExceptionSource() {
        return missingException("source");
    }


    private static Supplier<CommunicationException> missingException(String parameter) {
        return () -> new CommunicationException("There is a missing parameter in mongoDb authentication: " + parameter);
    }


}
