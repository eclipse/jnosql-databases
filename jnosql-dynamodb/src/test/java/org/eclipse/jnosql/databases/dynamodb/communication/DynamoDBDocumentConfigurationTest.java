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


import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DynamoDBDocumentConfigurationTest {

    @Test
    void shouldReturnFromServiceLoaderConfiguration() {
        var configuration = DocumentConfiguration.getConfiguration();
        Assertions.assertNotNull(configuration);
        Assertions.assertInstanceOf(DynamoDBDocumentConfiguration.class, configuration);
    }

    @Test
    void shouldReturnFromServiceLoaderConfigurationQuery() {
        var configuration = DocumentConfiguration
                .getConfiguration(DynamoDBDocumentConfiguration.class);
        Assertions.assertNotNull(configuration);
    }

    @Test
    void shouldReturnDocumentManagerFactory() {
        var configuration = DocumentConfiguration
                .getConfiguration(DynamoDBDocumentConfiguration.class);

        var settings = DynamoDBTestUtils.CONFIG.getSettings();

        assertSoftly(softly -> {
            softly.assertThat(configuration)
                    .describedAs("DocumentConfiguration.getConfiguration(DynamoDBDocumentConfiguration.class) must return a non-null instance")
                    .isNotNull();

            DocumentManagerFactory documentManagerFactory = configuration.apply(settings);

            softly.assertThat(documentManagerFactory)
                    .describedAs("DynamoDBDocumentConfiguration.apply(Settings.class) should returns a non-null DocumentManagerFactory instance")
                    .isNotNull();

        });
    }

}
