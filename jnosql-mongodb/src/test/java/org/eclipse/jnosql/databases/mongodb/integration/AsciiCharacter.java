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
 *   Maximillian Arruda
 */
package org.eclipse.jnosql.databases.mongodb.integration;

import jakarta.nosql.Column;
import jakarta.nosql.Entity;
import jakarta.nosql.Id;

@Entity
public record AsciiCharacter(
        @Id
        Long id,
        @Column
        Integer numericValue,
        @Column
        String hexadecimal,
        @Column
        Character thisCharacter,
        @Column
        Boolean isoControl) {

    public static boolean isValidAsciiCharacter(int numericPosition) {
        return numericPosition > 0 || numericPosition <= 127;
    }

    public static AsciiCharacter of(int value) {
        if (!isValidAsciiCharacter(value))
            throw new IllegalArgumentException(
                    "%d is not an ASCII character supported. It should be greater than 0 and less than 127."
                            .formatted(value));
        char thisCharacter = (char) value;
        return new AsciiCharacter(
                Integer.toUnsignedLong(value),
                value,
                Integer.toHexString(value),
                thisCharacter,
                Character.isISOControl(thisCharacter)
        );
    }

}
