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

package org.eclipse.jnosql.diana.infinispan.keyvalue.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Species implements Serializable {

    private static final long serialVersionUID = -1493508757572337719L;

    private final List<String> animals;

    public Species(String... animals) {
        this.animals = Arrays.asList(animals);
    }

    public List<String> getAnimals() {
        return animals;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Species species = (Species) o;
        return Objects.equals(animals, species.animals);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(animals);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Species{");
        sb.append("animals=").append(animals);
        sb.append('}');
        return sb.toString();
    }
}
