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
package org.eclipse.jnosql.communication.driver;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;
import javax.json.bind.config.PropertyVisibilityStrategy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

enum DefaultJsonbSupplier implements JsonbSupplier {

    INSTANCE;

    {
        JsonbConfig config = new JsonbConfig().withPropertyVisibilityStrategy(new PrivateVisibilityStrategy());
        this.json = JsonbBuilder.newBuilder().withConfig(config).build();
    }

    private final Jsonb json;

    @Override
    public Jsonb get() {
        return json;
    }


    static class PrivateVisibilityStrategy implements PropertyVisibilityStrategy {

        @Override
        public boolean isVisible(Field field) {
            return true;
        }

        @Override
        public boolean isVisible(Method method) {
            return true;
        }

    }
}
