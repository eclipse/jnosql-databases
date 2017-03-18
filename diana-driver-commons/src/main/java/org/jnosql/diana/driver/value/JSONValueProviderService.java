/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.driver.value;


import java.util.Objects;
import java.util.ServiceLoader;

public final class JSONValueProviderService {

    private static final JSONValueProvider PROVIDER;

    private JSONValueProviderService() {
    }

    static {
        JSONValueProvider aux = null;
        for (JSONValueProvider jsonValueProvider : ServiceLoader.load(JSONValueProvider.class)) {
            if (Objects.nonNull(jsonValueProvider)) {
                aux = jsonValueProvider;
            }
        }

        if (Objects.isNull(aux)) {
            PROVIDER = new JSONGSONValueProvider();
        } else {
            PROVIDER = aux;
        }

    }


    public static JSONValueProvider getProvider() {
        return new JSONGSONValueProvider();
    }
}
