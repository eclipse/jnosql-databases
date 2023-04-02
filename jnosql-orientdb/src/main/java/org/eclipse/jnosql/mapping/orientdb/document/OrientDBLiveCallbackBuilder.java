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
 *   Lucas Furlaneto
 */
package org.eclipse.jnosql.mapping.orientdb.document;

import org.eclipse.jnosql.communication.orientdb.document.OrientDBLiveCallback;
import org.eclipse.jnosql.communication.orientdb.document.OrientDBLiveCreateCallback;
import org.eclipse.jnosql.communication.orientdb.document.OrientDBLiveDeleteCallback;
import org.eclipse.jnosql.communication.orientdb.document.OrientDBLiveUpdateCallback;

import static java.util.Objects.requireNonNull;

public final class OrientDBLiveCallbackBuilder {
    private OrientDBLiveCreateCallback createCallback;
    private OrientDBLiveUpdateCallback updateCallback;
    private OrientDBLiveDeleteCallback deleteCallback;

    private OrientDBLiveCallbackBuilder() {
    }

    public static OrientDBLiveCallbackBuilder builder() {
        return new OrientDBLiveCallbackBuilder();
    }

    public <T> OrientDBLiveCallbackBuilder onCreate(OrientDBLiveCreateCallback<T> createCallback) {
        requireNonNull(createCallback, "createCallback is required");
        this.createCallback = createCallback;
        return this;
    }

    public <T> OrientDBLiveCallbackBuilder onUpdate(OrientDBLiveUpdateCallback<T> updateCallback) {
        requireNonNull(updateCallback, "updateCallback is required");
        this.updateCallback = updateCallback;
        return this;
    }

    public <T> OrientDBLiveCallbackBuilder onDelete(OrientDBLiveDeleteCallback<T> deleteCallback) {
        requireNonNull(deleteCallback, "deleteCallback is required");
        this.deleteCallback = deleteCallback;
        return this;
    }

    public OrientDBLiveCallback build() {
        validateNonNullCallbacks();
        return new OrientDBLiveCallback(createCallback, updateCallback, deleteCallback);
    }

    private void validateNonNullCallbacks() {
        if (createCallback == null && updateCallback == null && deleteCallback == null) {
            throw new IllegalArgumentException("At least one callback is required on OrientDB Live Query");
        }
    }
}