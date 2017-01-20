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
