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
package org.jnosql.diana.cassandra.column;


import org.jnosql.diana.api.column.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * The UDT builder
 */
public class UDTBuilder {

    private String name;

    private String typeName;

    private List<Column> columns = new ArrayList<>();

    UDTBuilder() {
    }

    /**
     * Set the name
     *
     * @param name the name
     * @return the builder instance
     * @throws NullPointerException when name is null
     */
    public UDTBuilder withName(String name) throws NullPointerException {
        this.name = Objects.requireNonNull(name, "name is required");
        return this;
    }

    /**
     * Set the typeName
     *
     * @param typeName the typeName
     * @return the builder instance
     * @throws NullPointerException when name is null
     */
    public UDTBuilder withTypeName(String typeName) throws NullPointerException {
        this.typeName = Objects.requireNonNull(typeName, "typeName is required");
        return this;
    }

    /**
     * add a column
     *
     * @param column a column
     * @return the builder instance
     * @throws NullPointerException when column is null
     */
    public UDTBuilder add(Column column) throws NullPointerException {
        columns.add(Objects.requireNonNull(column, "column is required"));
        return this;
    }


    /**
     * addd all columns elements
     *
     * @param columns the iterable to be added
     * @return the builder instance
     * @throws NullPointerException when either the columns or there is a null element
     */
    public UDTBuilder addAll(Iterable<Column> columns) throws NullPointerException {
        Objects.requireNonNull(columns, "columns is required");
        StreamSupport.stream(columns.spliterator(), false).forEach(this::add);
        return this;
    }

    /**
     * Creates a udt instance
     *
     * @return a udt instance
     * @throws IllegalStateException when there is a null element
     */
    public UDT build() throws IllegalStateException {
        if (Objects.isNull(name)) {
            throw new IllegalStateException("name is required");
        }
        if (Objects.isNull(typeName)) {
            throw new IllegalStateException("typeName is required");
        }
        return new DefaultUDT(name, typeName, columns);
    }
}
