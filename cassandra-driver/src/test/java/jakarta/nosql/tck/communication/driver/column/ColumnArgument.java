/*
 *  Copyright (c) 2020 Otávio Santana and others
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
package jakarta.nosql.tck.communication.driver.column;

import java.util.Collections;
import java.util.List;

public final class ColumnArgument {

    static final ColumnArgument EMPTY = new ColumnArgument();

    private final List<String> query;

    private final String idName;

    private final boolean empty;

    ColumnArgument(List<String> query, String idName) {
        this.query = query;
        this.idName = idName;
        this.empty = true;
    }

    ColumnArgument() {
        this.query = null;
        this.idName = null;
        this.empty = false;
    }


    public List<String> getQuery() {
        if (query == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(query);
    }

    public String getIdName() {
        return idName;
    }

    public boolean isEmpty() {
        return empty;
    }

    @Override
    public String toString() {
        return "ColumnArgument{" +
                "query=" + query +
                ", idName='" + idName + '\'' +
                ", empty=" + empty +
                '}';
    }
}
