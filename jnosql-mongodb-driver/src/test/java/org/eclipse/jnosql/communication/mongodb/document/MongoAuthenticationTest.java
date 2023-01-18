/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.mongodb.document;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCredential;
import org.eclipse.jnosql.communication.CommunicationException;
import org.eclipse.jnosql.communication.Settings;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static org.eclipse.jnosql.communication.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_MECHANISM;
import static org.eclipse.jnosql.communication.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_SOURCE;
import static org.eclipse.jnosql.communication.mongodb.document.MongoDBDocumentConfigurations.PASSWORD;
import static org.eclipse.jnosql.communication.mongodb.document.MongoDBDocumentConfigurations.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoAuthenticationTest {

    @Test
    public void shouldReturnErrorWhenTheNumberParameterIsInvalid() {
        Settings settings = Settings.builder().put(USER, "value")
                .build();

        assertThrows(CommunicationException.class, () -> MongoAuthentication.of(settings));

    }
    @Test
    public void shouldReturnOneAuthentication() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE, "database")
                .put(PASSWORD, "password")
                .put(USER, "user")
                .build();

        MongoCredential credential = MongoAuthentication.of(settings).get();
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertNull(credential.getMechanism());

    }

    @Test
    public void shouldReturnOneAuthenticationWithGSSAPI() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE, "database")
                .put(PASSWORD, "password")
                .put(USER, "user")
                .put(AUTHENTICATION_MECHANISM, "GSSAPI")
                .build();

        MongoCredential credential = MongoAuthentication.of(settings).get();
        assertEquals("$external", credential.getSource());
        assertEquals("user", credential.getUserName());
        assertEquals(GSSAPI.getMechanismName(), credential.getMechanism());

    }

    @Test
    public void shouldReturnOneAuthenticationWithMongoX509() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE, "database")
                .put(PASSWORD, "password")
                .put(USER, "user")
                .put(AUTHENTICATION_MECHANISM, "MONGODB-X509")
                .build();

        MongoCredential credential = MongoAuthentication.of(settings).get();
        assertEquals("$external", credential.getSource());
        assertEquals("user", credential.getUserName());
        assertEquals(AuthenticationMechanism.MONGODB_X509.getMechanismName(), credential.getMechanism());
    }

    @Test
    public void shouldReturnOneAuthenticationWithSCRAMSHA1() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE, "database")
                .put(PASSWORD, "password")
                .put(USER, "user")
                .put(AUTHENTICATION_MECHANISM, "SCRAM-SHA-1")
                .build();

        MongoCredential credential = MongoAuthentication.of(settings).get();
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertEquals(SCRAM_SHA_1.getMechanismName(), credential.getMechanism());
    }

    @Test
    public void shouldReturnOneAuthenticationWithSCRAMSHA256() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE, "database")
                .put(PASSWORD, "password")
                .put(USER, "user")
                .put(AUTHENTICATION_MECHANISM, "SCRAM-SHA-256")
                .build();

        MongoCredential credential = MongoAuthentication.of(settings).get();
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertEquals(SCRAM_SHA_256.getMechanismName(), credential.getMechanism());
    }

}