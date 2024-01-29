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
import net.datafaker.Faker;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.StringReader;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DynamoDBConverterTest {

    static final Faker faker = new Faker();

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();

    private static final UnaryOperator<String> entityNameResolver = UnaryOperator.identity();

    @Test
    void shouldConvertToItemRequest() {

        assertSoftly(softly -> {
            var entity = DocumentEntity.of("entityA",
                    List.of(
                            Document.of("_id", UUID.randomUUID().toString()),
                            Document.of("city", faker.address().city()),
                            Document.of("total", 10.0),
                            Document.of("address", List.of(
                                    Document.of("zipcode", faker.address().zipCode()),
                                    Document.of("city", faker.address().cityName()))),
                            Document.of("phones", List.of(faker.name().firstName(), faker.name().firstName(), faker.name().firstName()))
                    ));

            var item = DynamoDBConverter.toItem(entityNameResolver, entity);

            var entityFromItem = DynamoDBConverter.toDocumentEntity(entityNameResolver, item);

            var expected = Json.createReader(new StringReader(JSONB.toJson(DynamoDBConverter.getMap(entityNameResolver, entity)))).readObject();

            var actual = Json.createReader(new StringReader(JSONB.toJson(DynamoDBConverter.getMap(entityNameResolver, entityFromItem)))).readObject();

            softly.assertThat(actual).as("cannot convert a simple DocumentEntity")
                    .isEqualTo(expected);
        });


    }

}
