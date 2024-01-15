package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import org.junit.jupiter.api.Test;

public class TestHelloWorld {

    @Test
    void shouldCreateNoSQLHandle() throws Exception {
        NoSQLHandle serviceHandle = Database.INSTANCE.getNoSQLHandle();
        TableRequest req = new TableRequest().setStatement(
                "CREATE TABLE if not exists hello_world(id LONG, " +
                        "content JSON, primary key (id))");

        req.setTableLimits(new TableLimits(25, 25, 25));

        TableResult tr = serviceHandle.tableRequest(req);
        tr.waitForCompletion(serviceHandle, 120000, 500);
        if (tr.getTableState() != TableResult.State.ACTIVE)  {
            throw new Exception("Unable to create table hello_world " +
                    tr.getTableState());
        }
    }
}
