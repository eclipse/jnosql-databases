/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.driver;

/**
 * A structured class to hold both the test parameter and its expected value.
 * For now, we'll leave this test variable here since the cost of creating a project for a single configuration
 * is too high. And there isn't a big cost of this class on runtime.
 */
public final class IntegrationTest {

    /**
     * The {@link System#getProperty(String)} key to find
     */
    public final static String INTEGRATION = "jnosql.test.integration";

    /**
     * The match value on the integration test
     */
    public final static String INTEGRATION_MATCHES = "true";

    private IntegrationTest() {
    }
}
