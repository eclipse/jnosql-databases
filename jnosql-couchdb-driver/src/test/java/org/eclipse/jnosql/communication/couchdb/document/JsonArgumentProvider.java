/*
 *
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
 *
 */
package org.eclipse.jnosql.communication.couchdb.document;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;

import jakarta.json.Json;
import jakarta.json.JsonReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonArgumentProvider implements ArgumentsProvider, AnnotationConsumer<JsonSource> {

    private String file;

    @Override
    public void accept(JsonSource jsonSource) {
        this.file = jsonSource.value();
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        InputStream stream = JsonArgumentProvider.class.getClassLoader().getResourceAsStream(file);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String json = reader.lines().collect(Collectors.joining());
            JsonReader jsonReader = Json.createReader(new StringReader(json));
            return Stream.of(Arguments.of(jsonReader.readObject()));
        }

    }
}
