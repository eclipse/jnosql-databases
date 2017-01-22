/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.driver;


import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class ConfigurationReader {

    private static final Logger LOGGER = Logger.getLogger(ConfigurationReader.class.getName());

    private ConfigurationReader() {
    }

    public static Map<String, String> from(String resource) throws NullPointerException {
        Objects.requireNonNull(resource, "Resource is required");

        try {
            Properties properties = new Properties();
            InputStream stream = ConfigurationReader.class.getClassLoader()
                    .getResourceAsStream(resource);
            properties.load(stream);
            return properties.keySet().stream().collect(Collectors
                    .toMap(Object::toString, s -> properties.get(s).toString()));
        } catch (IOException e) {
            LOGGER.fine("The file was not found: " + resource);
            return Collections.emptyMap();
        }
    }
}
