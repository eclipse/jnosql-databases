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
package org.eclipse.jnosql.diana.hazelcast.keyvalue.model;

import java.io.Serializable;

public class Movie implements Serializable {

    private String name;

    private Integer year;

    private boolean active;


    public Movie(String name, Integer year, boolean active) {
        this.name = name;
        this.year = year;
        this.active = active;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Movie{");
        sb.append("name='").append(name).append('\'');
        sb.append(", year=").append(year);
        sb.append(", active=").append(active);
        sb.append('}');
        return sb.toString();
    }
}
