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
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;

import java.io.StringReader;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DocumentEntityConverterTest {

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();
    private static final UnaryOperator<String> entityNameResolver = UnaryOperator.identity();


    @Test
    void shouldConvertDocumentEntityToEnhancedDocument() {

        assertSoftly(softly -> {
            var entity = DocumentEntityGenerator.getEntity();
            var enhancedDocument = DocumentEntityConverter.toEnhancedDocument(entityNameResolver, entity);
            var expected = Json.createReader(new StringReader(JSONB.toJson(DocumentEntityConverter.getMap(entityNameResolver, entity)))).readObject();
            var actual = Json.createReader(new StringReader(enhancedDocument.toJson())).readObject();
            softly.assertThat(actual).as("cannot convert a simple DocumentEntity")
                    .isEqualTo(expected);
        });
    }

    @Test
    void shouldConvertDocumentEntityWithSubDocumentsToEnhancedDocument() {

        assertSoftly(softly -> {
            var entity = DocumentEntityGenerator.getEntityWithSubDocuments(3);
            var enhancedDocument = DocumentEntityConverter.toEnhancedDocument(entityNameResolver, entity);
            var expected = Json.createReader(new StringReader(JSONB.toJson(DocumentEntityConverter.getMap(entityNameResolver, entity)))).readObject();
            var actual = Json.createReader(new StringReader(enhancedDocument.toJson())).readObject();
            softly.assertThat(actual).as("cannot convert a DocumentEntity with document sublist")
                    .isEqualTo(expected);
        });
    }


    @Test
    void shouldConvertEnhancedDocumentToDocumentEntity() {

        var enhancedDocument = EnhancedDocument.builder()
                .json("""
                        {
                            "%s":"Max",
                            "%s": "person",
                            "name":"Maximillian",
                            "number": 123,
                            "address": {
                                "street": "Rua tralala"
                            },
                            "phones": [
                                "1111-2222",
                                "2222-3333"
                            ]
                        }
                        """.formatted(DocumentEntityConverter.ID, DocumentEntityConverter.ENTITY)).build();
        var expected = DocumentEntityConverter.toDocumentEntity(entityNameResolver, enhancedDocument);

        assertSoftly(softly -> {
            softly.assertThat(expected).as("cannot return a null reference")
                    .isNotNull();
            softly.assertThat(expected.name()).as("documentEntity name is not correct")
                    .isEqualTo("person");

            softly.assertThat(expected.find("_id", String.class))
                    .as("documentEntity._id was parsed incorrectly")
                    .hasValue("Max");
            softly.assertThat(expected.find("name", String.class))
                    .as("documentEntity.name was parsed incorrectly")
                    .hasValue("Maximillian");
            softly.assertThat(expected.find("number", Integer.class))
                    .as("documentEntity.number was parsed incorrectly")
                    .hasValue(123);
            softly.assertThat(expected.find("address", new TypeReference<List<Document>>() {
                    }))
                    .as("documentEntity.address was parsed incorrectly")
                    .contains(List.of(Document.of("street", "Rua tralala")));
            softly.assertThat(expected.find("phones", new TypeReference<List<String>>() {
                    }))
                    .as("documentEntity.phones  was parsed incorrectly")
                    .contains(List.of("1111-2222", "2222-3333"));

        });
    }


}
