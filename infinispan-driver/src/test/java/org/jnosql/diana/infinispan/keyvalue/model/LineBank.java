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

package org.jnosql.diana.infinispan.keyvalue.model;


import java.io.Serializable;
import java.util.Objects;

public class LineBank implements Serializable {


    private final Person person;

    public Person getPerson() {
        return person;
    }

    public LineBank(String name, Integer age) {
        this.person = new Person(name, age);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LineBank lineBank = (LineBank) o;
        return Objects.equals(person, lineBank.person);
    }

    @Override
    public int hashCode() {
        return Objects.hash(person);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LineBank{");
        sb.append("person=").append(person);
        sb.append('}');
        return sb.toString();
    }
}
