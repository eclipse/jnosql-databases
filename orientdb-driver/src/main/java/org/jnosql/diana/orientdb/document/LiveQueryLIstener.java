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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.function.Consumer;

class LiveQueryLIstener implements OLiveResultListener {

    private final Consumer<DocumentEntity> entityConsumer;

    LiveQueryLIstener(Consumer<DocumentEntity> entityConsumer) {
        this.entityConsumer = entityConsumer;
    }

    @Override
    public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
        ODocument oDocument = (ODocument) iOp.getRecord();
        DocumentEntity entity = OrientDBConverter.convert(oDocument);
        entityConsumer.accept(entity);

    }

    @Override
    public void onError(int iLiveToken) {
    }

    @Override
    public void onUnsubscribe(int iLiveToken) {
    }
}
