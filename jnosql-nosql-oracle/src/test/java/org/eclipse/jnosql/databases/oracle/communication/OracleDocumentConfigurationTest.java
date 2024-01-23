/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OracleDocumentConfigurationTest {

    @Test
    void shouldReturnErrorWhenMapSettingsIsNull() {
        OracleDocumentConfiguration configuration = new OracleDocumentConfiguration();
        assertThrows(NullPointerException.class, () -> configuration.apply(null));
    }

    @Test
    void shouldReturnFromConfiguration() {
        DocumentConfiguration configuration = DocumentConfiguration.getConfiguration();
        assertThat(configuration).isNotNull()
                .isInstanceOf(DocumentConfiguration.class)
                .isInstanceOf(OracleDocumentConfiguration.class);
    }
}
