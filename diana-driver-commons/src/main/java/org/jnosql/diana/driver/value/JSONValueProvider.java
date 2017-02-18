/*
 * Copyright 2017 Otavio Santana and others
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
package org.jnosql.diana.driver.value;


import org.jnosql.diana.api.Value;

/**
 * The provider to create Value storage as JSON objetc
 */
public interface JSONValueProvider {

    /**
     * Creates a {@link Value} that storage a json text
     *
     * @param json a json text
     * @return the {@link Value} instance
     * @throws NullPointerException          when the json is null
     * @throws UnsupportedOperationException when this method is not supported
     */
    Value of(String json) throws NullPointerException, UnsupportedOperationException;


    /**
     * Creates a {@link Value} that storage a json byte array
     *
     * @param json a json byte array
     * @return the {@link Value} instance
     * @throws NullPointerException          when the json is null
     * @throws UnsupportedOperationException when this method is not supported
     */
    Value of(byte[] json) throws NullPointerException, UnsupportedOperationException;


    /**
     * converts an object to json text
     *
     * @param object a object instance
     * @return the json
     * @throws NullPointerException          when the json is null
     * @throws UnsupportedOperationException when this method is not supported
     */
    String toJson(Object object) throws NullPointerException, UnsupportedOperationException;


    /**
     * converts an object to byte array
     *
     * @param object a object instance
     * @return the json
     * @throws NullPointerException          when the json is null
     * @throws UnsupportedOperationException when this method is not supported
     */
    byte[] toJsonArray(Object object) throws NullPointerException, UnsupportedOperationException;

}
