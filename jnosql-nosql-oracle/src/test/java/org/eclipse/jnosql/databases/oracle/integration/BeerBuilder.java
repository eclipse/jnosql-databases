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

import java.util.List;
import java.util.Map;

public class BeerBuilder {
    private String id;
    private List<String> comments;
    private List<Crew> crew;
    private Map<String, Object> data;

    public BeerBuilder id(String id) {
        this.id = id;
        return this;
    }

    public BeerBuilder comments(List<String> comments) {
        this.comments = comments;
        return this;
    }

    public BeerBuilder crew(List<Crew> crew) {
        this.crew = crew;
        return this;
    }

    public BeerBuilder data(Map<String, Object> data) {
        this.data = data;
        return this;
    }

    public Beer build() {
        return new Beer(id, comments, crew, data);
    }
}