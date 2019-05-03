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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_SOURCE;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_MECHANISM;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.PASSWORD;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.USER;

final class MongoAuthentication {

    private final String user;

    private final String database;

    private final char[] password;

    private final AuthenticationMechanism mechanism;

    private MongoAuthentication(String user, String database, char[] password,
                                AuthenticationMechanism mechanism) {
        this.user = user;
        this.database = database;
        this.password = password;
        this.mechanism = mechanism;
    }

    MongoCredential toCredential() {
        switch (mechanism) {
            case PLAIN:
                return MongoCredential.createPlainCredential(user, database, password);
            case GSSAPI:
                return MongoCredential.createGSSAPICredential(user);
            case SCRAM_SHA_1:
                return MongoCredential.createScramSha1Credential(user, database, password);
            case MONGODB_X509:
                return MongoCredential.createMongoX509Credential(user);
            case SCRAM_SHA_256:
                return MongoCredential.createScramSha256Credential(user, database, password);
            default:
                throw new JNoSQLException("There is not support to the type: " + mechanism);

        }
    }


    static List<MongoAuthentication> of(Settings settings) {

        List<MongoAuthentication> authentications = new ArrayList<>();

        List<String> users = settings.prefix(Arrays.asList(USER.get(),
                Configurations.USER.get())).stream()
                .map(Object::toString).collect(Collectors.toList());

        List<String> passwords = settings.prefix(Arrays.asList(PASSWORD.get(),
                Configurations.PASSWORD.get())).stream()
                .map(Object::toString).collect(Collectors.toList());

        List<String> sources = settings.prefix(AUTHENTICATION_SOURCE.get()).stream()
                .map(Object::toString).collect(Collectors.toList());

        AuthenticationMechanism mechanism = settings.get(AUTHENTICATION_MECHANISM.get())
                .map(Object::toString)
                .map(AuthenticationMechanism::fromMechanismName)
                .orElse(AuthenticationMechanism.PLAIN);

        if (users.size() != passwords.size() || users.size() != sources.size()) {
            throw new JNoSQLException("There is an inconsistent number of authentication parameter");
        }

        for (int index = 0; index < sources.size(); index++) {
            String user = users.get(index);
            String password = passwords.get(index);
            String source = sources.get(index);
            MongoAuthentication authentication = new MongoAuthentication(user,
                    source, password.toCharArray(), mechanism);
            authentications.add(authentication);
        }

        return authentications;

    }
}
