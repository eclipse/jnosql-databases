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
package org.eclipse.jnosql.communication.driver;

import javax.json.bind.Jsonb;
import java.util.function.Supplier;

/**
 * Defines a supplier to {@link Jsonb} already configured and ready to use in the drivers whose need a JSON processor.
 */
public interface JsonbSupplier extends Supplier<Jsonb> {

    /**
     * It returns a {@link JsonbSupplier} from {@link java.util.ServiceLoader} otherwise,
     * it will return the default JsonbSupplier that reads from the field instead of the method.
     *
     * @return {@link JsonbSupplier} instance
     */
    static JsonbSupplier getInstance() {
        return JsonbSupplierServiceLoader.getInstance();
    }
}
