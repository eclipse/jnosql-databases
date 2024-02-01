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
package org.eclipse.jnosql.databases.oracle.integration;

import jakarta.nosql.Column;
import jakarta.nosql.Entity;
import jakarta.nosql.Id;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Entity
public class Beer {

    @Id
    private String id;

    @Column
    private List<String> comments;

    @Column
    public List<Crew> crew;

    @Column
    public Map<String, Object> data;


    public String id() {
        return id;
    }

    public List<String> comments() {
        return comments;
    }

    public List<Crew> crew() {
        return crew;
    }

    public Map<String, Object> data() {
        return data;
    }

    Beer() {
    }

    Beer(String id, List<String> comments, List<Crew> crew, Map<String, Object> data) {
        this.id = id;
        this.comments = comments;
        this.crew = crew;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Beer{" +
                "id='" + id + '\'' +
                ", comments=" + comments +
                ", crew=" + crew +
                ", data=" + data +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Beer beer = (Beer) o;
        return Objects.equals(id, beer.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    public static BeerBuilder builder() {
        return new BeerBuilder();
    }
}
