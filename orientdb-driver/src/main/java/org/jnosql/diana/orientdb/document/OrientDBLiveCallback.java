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

import org.jnosql.diana.api.document.DocumentEntity;

public class OrientDBLiveCallback {

    private final OrientDBLiveCreateCallback<DocumentEntity> createCallback;
    private final OrientDBLiveUpdateCallback<DocumentEntity> updateCallback;
    private final OrientDBLiveDeleteCallback<DocumentEntity> deleteCallback;

    public OrientDBLiveCallback(OrientDBLiveCreateCallback<DocumentEntity> createCallback,
                                OrientDBLiveUpdateCallback<DocumentEntity> updateCallback,
                                OrientDBLiveDeleteCallback<DocumentEntity> deleteCallback) {
        this.createCallback = createCallback;
        this.updateCallback = updateCallback;
        this.deleteCallback = deleteCallback;
    }

    public OrientDBLiveCreateCallback<DocumentEntity> getCreateCallback() {
        return createCallback;
    }

    public OrientDBLiveUpdateCallback<DocumentEntity> getUpdateCallback() {
        return updateCallback;
    }

    public OrientDBLiveDeleteCallback<DocumentEntity> getDeleteCallback() {
        return deleteCallback;
    }

    public static OrientDBLiveCallbackBuilder builder() {
        return OrientDBLiveCallbackBuilder.builder();
    }
}
