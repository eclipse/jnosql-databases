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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import org.jnosql.diana.api.document.DocumentEntity;

import static com.orientechnologies.orient.core.db.record.ORecordOperation.CREATED;
import static com.orientechnologies.orient.core.db.record.ORecordOperation.DELETED;
import static com.orientechnologies.orient.core.db.record.ORecordOperation.UPDATED;

class LiveQueryLIstener implements OLiveResultListener {

    private final OrientDBLiveCallback callbacks;

    LiveQueryLIstener(OrientDBLiveCallback callbacks) {
        this.callbacks = callbacks;
    }

    @Override
    public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
        ODocument oDocument = (ODocument) iOp.getRecord();
        DocumentEntity entity = OrientDBConverter.convert(oDocument);

        switch (iOp.type) {
            case CREATED:
                callbacks.getCreateCallback().ifPresent(callback -> callback.accept(entity));
                return;
            case UPDATED:
                callbacks.getUpdateCallback().ifPresent(callback -> callback.accept(entity));
                return;
            case DELETED:
                callbacks.getDeleteCallback().ifPresent(callback -> callback.accept(entity));
                return;
            default:
        }
    }

    @Override
    public void onError(int iLiveToken) {
    }

    @Override
    public void onUnsubscribe(int iLiveToken) {
    }
}
