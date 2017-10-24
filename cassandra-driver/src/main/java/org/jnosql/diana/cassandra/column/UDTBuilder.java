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

    private List<List<Column>> subColumns = new ArrayList<>();

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
     * Adds the udt when the type is just one element
     *
     * @param udt the elements in a UDT to be added
     * @return the builder instance
     * @throws NullPointerException when either the udt or there is a null element
     */
    public UDTBuilder addAll(Iterable<Column> udt) throws NullPointerException {
        Objects.requireNonNull(udt, "udt is required");
        StreamSupport.stream(udt.spliterator(), false).forEach(this.columns::add);
        return this;
    }

    /**
     * <p>On Cassandra, there is the option to a UDT be part of a list. This implementation holds this option.</p>
     * <p>eg: CREATE COLUMNFAMILY IF NOT EXISTS contacts ( user text PRIMARY KEY, names list<frozen <fullname>>);</p>
     *
     * @param udts the UTDs to be added
     * @return the builder instance
     * @throws NullPointerException when either the udt or there is a null element
     */
    public UDTBuilder addElement(Iterable<Iterable<Column>> udts) throws NullPointerException, IllegalStateException {
        Objects.requireNonNull(udts, "udts is required");
        for (Iterable<Column> subColumn : udts) {
            List<Column> ts = new ArrayList<>();
            for (Column column : subColumn) {
                ts.add(column);
            }
            this.subColumns.add(ts);
        }
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

