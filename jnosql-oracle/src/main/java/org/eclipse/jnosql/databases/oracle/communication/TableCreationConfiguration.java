/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import org.eclipse.jnosql.communication.CommunicationException;

import java.util.logging.Logger;

record TableCreationConfiguration(int readLimit,
                                  int writeLimit,
                                  int storageGB,
                                  int waitMillis,
                                  int delayMillis) {

    private static final String CREATE_TABLE ="CREATE TABLE if not exists %s (id STRING, content JSON, primary key (id))";
    private static final Logger LOGGER = Logger.getLogger(OracleBucketManagerFactory.class.getName());
    static final String JSON_FIELD = "content";

    static final String ID_FIELD = "id";

    public void createTable(String tableName, NoSQLHandle serviceHandle){
        String table = String.format(CREATE_TABLE, tableName);
        LOGGER.info("starting the bucket manager, creating a table Running query: " + table);
        TableRequest tableRequest = new TableRequest().setStatement(table);
        tableRequest.setTableLimits(new TableLimits(this.readLimit, this.writeLimit, this.storageGB));
        TableResult tableResult = serviceHandle.tableRequest(tableRequest);
        tableResult.waitForCompletion(serviceHandle, this.waitMillis, this.delayMillis);
        if (tableResult.getTableState() != TableResult.State.ACTIVE)  {
            throw new CommunicationException("Unable to create table "+ tableName +
                    tableResult.getTableState());
        }
    }
}
