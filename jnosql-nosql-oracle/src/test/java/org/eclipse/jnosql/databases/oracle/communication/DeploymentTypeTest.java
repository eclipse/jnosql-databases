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
package org.eclipse.jnosql.databases.oracle.communication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

class DeploymentTypeTest {


    @Test
    void shouldReturnOnPremiseWhenIsNull() {
        assertEquals(DeploymentType.ON_PREMISES, DeploymentType.parse(null));
    }

    @Test
    void shouldReturnOnPremiseWhenTextIsInvalid() {
        assertEquals(DeploymentType.ON_PREMISES, DeploymentType.parse("invalid"));
    }

    @ParameterizedTest
    @EnumSource(DeploymentType.class)
    void shouldParseEnum(DeploymentType type){
        assertEquals(type, DeploymentType.parse(type.name()));
    }

    @ParameterizedTest
    @EnumSource(DeploymentType.class)
    void shouldParseEnumIgnoreCase(DeploymentType type){
        assertEquals(type, DeploymentType.parse(type.name().toLowerCase(Locale.ROOT)));
    }
}