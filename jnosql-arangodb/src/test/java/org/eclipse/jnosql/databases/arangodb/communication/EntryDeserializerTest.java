/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.arangodb.communication;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntryDeserializerTest {


    @Test
    public void shouldReturnErrorWhenNameIsNull() {
        assertThrows(NullPointerException.class, () -> EntryDeserializer.of(null));
    }

    @Test
    public void shouldReturnErrorWhenInstanceIsNotSerializer() {
        assertThrows(IllegalArgumentException.class, () -> EntryDeserializer.of(String.class.getName()));
    }

    @Test
    public void shouldCreateEntryDeserializer() throws Exception {
        EntryDeserializer<Money> serializer = EntryDeserializer.of(MoneyJsonDeserializer.class.getName());
        assertNotNull(serializer);

        SoftAssertions.assertSoftly(s -> {
            s.assertThat(serializer.deserializer()).isNotNull().isInstanceOf(MoneyJsonDeserializer.class);
            s.assertThat(serializer.type()).isEqualTo(Money.class);
        });
    }
}