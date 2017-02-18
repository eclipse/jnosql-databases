/*
 * Copyright 2017 Otavio Santana and others
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
