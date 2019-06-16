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
 *   Lucas Furlaneto
 */
package org.jnosql.diana.orientdb.document;

import jakarta.nosql.document.DocumentEntity;

import static java.util.Objects.requireNonNull;

public final class OrientDBLiveCallbackBuilder {
    private OrientDBLiveCreateCallback<DocumentEntity> createCallback;
    private OrientDBLiveUpdateCallback<DocumentEntity> updateCallback;
    private OrientDBLiveDeleteCallback<DocumentEntity> deleteCallback;

    private OrientDBLiveCallbackBuilder() {
    }

    public static OrientDBLiveCallbackBuilder builder() {
        return new OrientDBLiveCallbackBuilder();
    }

    public OrientDBLiveCallbackBuilder onCreate(OrientDBLiveCreateCallback<DocumentEntity> createCallback) {
        requireNonNull(createCallback, "createCallback is required");
        this.createCallback = createCallback;
        return this;
    }

    public OrientDBLiveCallbackBuilder onUpdate(OrientDBLiveUpdateCallback<DocumentEntity> updateCallback) {
        requireNonNull(updateCallback, "updateCallback is required");
        this.updateCallback = updateCallback;
        return this;
    }

    public OrientDBLiveCallbackBuilder onDelete(OrientDBLiveDeleteCallback<DocumentEntity> deleteCallback) {
        requireNonNull(deleteCallback, "deleteCallback is required");
        this.deleteCallback = deleteCallback;
        return this;
    }

    public OrientDBLiveCallback<DocumentEntity> build() {
        validateNonNullCallbacks();
        return new OrientDBLiveCallback<>(createCallback, updateCallback, deleteCallback);
    }

    private void validateNonNullCallbacks() {
        if (createCallback == null && updateCallback == null && deleteCallback == null) {
            throw new IllegalArgumentException("At least one callback is required on OrientDB Live Query");
        }
    }
}