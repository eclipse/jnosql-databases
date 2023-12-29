/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.communication;

import jakarta.json.Json;
import jakarta.json.bind.Jsonb;
import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.StringReader;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DynamoDBDocumentEntityConverterTest {

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();


    @Test
    void shouldConvertDocumentEntityToEnhancedDocument() {

        assertSoftly(softly -> {
            var entity = DocumentEntityGenerator.getEntity();
            var enhancedDocument = DynamoDBDocumentEntityConverter.toEnhancedDocument(entity);
            var expected = Json.createReader(new StringReader(JSONB.toJson(DynamoDBDocumentEntityConverter.getMap(entity)))).readObject();
            var actual = Json.createReader(new StringReader(enhancedDocument.toJson())).readObject();
            softly.assertThat(actual).as("cannot convert a simple DocumentEntity")
                    .isEqualTo(expected);
        });
    }

    @Test
    void shouldConvertDocumentEntityWithSubDocumentsToEnhancedDocument() {

        assertSoftly(softly -> {
            var entity = DocumentEntityGenerator.getEntityWithSubDocuments(3);
            var enhancedDocument = DynamoDBDocumentEntityConverter.toEnhancedDocument(entity);
            var expected = Json.createReader(new StringReader(JSONB.toJson(DynamoDBDocumentEntityConverter.getMap(entity)))).readObject();
            var actual = Json.createReader(new StringReader(enhancedDocument.toJson())).readObject();
            softly.assertThat(actual).as("cannot convert a DocumentEntity with document sublist")
                    .isEqualTo(expected);
        });
    }


}
