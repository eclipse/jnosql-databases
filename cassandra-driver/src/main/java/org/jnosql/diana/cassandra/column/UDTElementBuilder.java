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

public interface UDTElementBuilder {


    /**
     * Adds the udt when the type is just one element
     *
     * @param udt the elements in a UDT to be added
     * @return the builder instance
     * @throws NullPointerException when either the udt or there is a null element
     */
    UDTFinisherBuilder addUDT(Iterable<Column> udt) throws NullPointerException;
    /**
     * <p>On Cassandra, there is the option to a UDT be part of a list. This implementation holds this option.</p>
     * <p>eg: CREATE COLUMNFAMILY IF NOT EXISTS contacts ( user text PRIMARY KEY, names list&#60;frozen &#60;fullname&#62;&#62;);</p>
     *
     * @param udts the UTDs to be added
     * @return the builder instance
     * @throws NullPointerException when either the udt or there is a null element
     */
    UDTFinisherBuilder addUDTs(Iterable<Iterable<Column>> udts) throws NullPointerException;
}
