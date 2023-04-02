/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.cassandra.mapping.column.model;

import jakarta.nosql.Column;
import jakarta.nosql.Entity;
import jakarta.nosql.Id;
import org.eclipse.jnosql.mapping.MappedSuperclass;

import java.util.List;

@Entity
@MappedSuperclass
public class Artist {

    @Id
    private long id;

    @Column
    private String name;
    @Column
    private int age;
    @Column
    private List<String> phones;
    @Column
    private String ignore;

    Artist() {
    }

    Artist(long id, String name, int age, List<String> phones, String ignore) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.phones = phones;
        this.ignore = ignore;
    }


    public static ArtistBuilder builder() {
        return new ArtistBuilder();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public List<String> getPhones() {
        return phones;
    }

    public String getIgnore() {
        return ignore;
    }
}
