/*
 *  Copyright (c) 2017 Otávio Santana and others
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
