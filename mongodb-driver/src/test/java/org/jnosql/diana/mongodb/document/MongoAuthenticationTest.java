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
import org.jnosql.diana.api.JNoSQLException;
import org.jnosql.diana.api.Settings;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_SOURCE;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.PASSWORD;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.USER;
import static org.jnosql.diana.mongodb.document.MongoDBDocumentConfigurations.AUTHENTICATION_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoAuthenticationTest {

    //deve criar um para cada tipo
    //deve retornar erro quando os authen forem diferentes
    //deve retorna quando tiver um de cada
    //deve retornar quando tiver
    //quando for default

    @Test
    public void shouldReturnErrorWhenTheNumberParameterIsInvalid() {
        Settings settings = Settings.builder().put(AUTHENTICATION_SOURCE.get(), "value")
                .build();

        assertThrows(JNoSQLException.class, () -> MongoAuthentication.of(settings));

    }

    @Test
    public void shouldReturnOneAuthentication() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE.get(), "database")
                .put(PASSWORD.get(), "password")
                .put(USER.get(), "user")
                .build();

        List<MongoCredential> credentials = MongoAuthentication.of(settings);
        assertEquals(1, credentials.size());
        MongoCredential credential = credentials.get(0);
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertEquals(PLAIN.getMechanismName(), credential.getMechanism());

    }

    @Test
    public void shouldReturnOneAuthenticationWithGSSAPI() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE.get(), "database")
                .put(PASSWORD.get(), "password")
                .put(USER.get(), "user")
                .put(AUTHENTICATION_MECHANISM.get(), "GSSAPI")
                .build();

        List<MongoCredential> credentials = MongoAuthentication.of(settings);
        assertEquals(1, credentials.size());
        MongoCredential credential = credentials.get(0);
        assertEquals("$external", credential.getSource());
        assertEquals("user", credential.getUserName());
        assertEquals(GSSAPI.getMechanismName(), credential.getMechanism());

    }

    @Test
    public void shouldReturnOneAuthenticationWithMongoX509() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE.get(), "database")
                .put(PASSWORD.get(), "password")
                .put(USER.get(), "user")
                .put(AUTHENTICATION_MECHANISM.get(), "MONGODB-X509")
                .build();

        List<MongoCredential> credentials = MongoAuthentication.of(settings);
        assertEquals(1, credentials.size());
        MongoCredential credential = credentials.get(0);
        assertEquals("$external", credential.getSource());
        assertEquals("user", credential.getUserName());
        assertEquals(AuthenticationMechanism.MONGODB_X509.getMechanismName(), credential.getMechanism());
    }

    @Test
    public void shouldReturnOneAuthenticationWithSCRAMSHA1() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE.get(), "database")
                .put(PASSWORD.get(), "password")
                .put(USER.get(), "user")
                .put(AUTHENTICATION_MECHANISM.get(), "SCRAM-SHA-1")
                .build();

        List<MongoCredential> credentials = MongoAuthentication.of(settings);
        assertEquals(1, credentials.size());
        MongoCredential credential = credentials.get(0);
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertEquals(SCRAM_SHA_1.getMechanismName(), credential.getMechanism());
    }

    @Test
    public void shouldReturnOneAuthenticationWithSCRAMSHA256() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE.get(), "database")
                .put(PASSWORD.get(), "password")
                .put(USER.get(), "user")
                .put(AUTHENTICATION_MECHANISM.get(), "SCRAM-SHA-256")
                .build();

        List<MongoCredential> credentials = MongoAuthentication.of(settings);
        assertEquals(1, credentials.size());
        MongoCredential credential = credentials.get(0);
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertEquals(SCRAM_SHA_256.getMechanismName(), credential.getMechanism());
    }

    @Test
    public void shouldReturnAuthentications() {
        Settings settings = Settings.builder()
                .put(AUTHENTICATION_SOURCE.get()+".1", "database")
                .put(PASSWORD.get()+".1", "password")
                .put(USER.get()+".1", "user")
                .put(AUTHENTICATION_SOURCE.get()+".2", "database-2")
                .put(PASSWORD.get()+".2", "password-2")
                .put(USER.get()+".2", "user-2")
                .build();

        List<MongoCredential> credentials = MongoAuthentication.of(settings);
        assertEquals(2, credentials.size());
        MongoCredential credential = credentials.get(0);
        assertEquals("database", credential.getSource());
        assertTrue(Arrays.equals("password".toCharArray(), credential.getPassword()));
        assertEquals("user", credential.getUserName());
        assertEquals(PLAIN.getMechanismName(), credential.getMechanism());

        credential = credentials.get(1);
        assertEquals("database-2", credential.getSource());
        assertTrue(Arrays.equals("password-2".toCharArray(), credential.getPassword()));
        assertEquals("user-2", credential.getUserName());
        assertEquals(PLAIN.getMechanismName(), credential.getMechanism());

    }
}