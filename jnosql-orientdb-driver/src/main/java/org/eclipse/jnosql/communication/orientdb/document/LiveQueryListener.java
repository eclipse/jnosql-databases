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
package org.eclipse.jnosql.communication.orientdb.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import jakarta.nosql.document.DocumentEntity;

class LiveQueryListener implements OLiveQueryResultListener {

    private final OrientDBLiveCallback<DocumentEntity> callbacks;
    private final ODatabaseSession tx;

    LiveQueryListener(OrientDBLiveCallback<DocumentEntity> callbacks, ODatabaseSession tx) {
        this.callbacks = callbacks;
        this.tx = tx;
    }

    @Override
    public void onCreate(ODatabaseDocument database, OResult data) {
        DocumentEntity entity = OrientDBConverter.convert(data);
        callbacks.getCreateCallback().ifPresent(callback -> callback.accept(entity));
    }

    @Override
    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
        DocumentEntity entity = OrientDBConverter.convert(after);
        callbacks.getCreateCallback().ifPresent(callback -> callback.accept(entity));
    }

    @Override
    public void onDelete(ODatabaseDocument database, OResult data) {
        DocumentEntity entity = OrientDBConverter.convert(data);
        callbacks.getCreateCallback().ifPresent(callback -> callback.accept(entity));
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {
        System.out.printf("error");
    }

    @Override
    public void onEnd(ODatabaseDocument database) {
        tx.close();
    }
}
