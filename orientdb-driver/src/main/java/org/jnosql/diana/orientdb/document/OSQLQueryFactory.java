/*
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


import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.function.Consumer;

final class OSQLQueryFactory {

    private OSQLQueryFactory() {
    }

    public static OSQLQuery<ODocument> to(DocumentQuery documentQuery) {
        String query = getQuery(documentQuery);
        return new OSQLQuery<ODocument>(query) {
        };
    }

    public static OSQLAsynchQuery<ODocument> toAsync(DocumentQuery documentQuery, Consumer<Void> callBack) {
        String query = getQuery(documentQuery);
        return new OSQLAsynchQuery<ODocument>(query, new OCommandResultListener() {
            @Override
            public boolean result(Object iRecord) {
                return false;
            }

            @Override
            public void end() {
                callBack.accept(null);
            }

            @Override
            public Object getResult() {
                return null;
            }
        });
    }

    private static String getQuery(DocumentQuery documentQuery) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT FROM ").append(documentQuery.getCollection()).append(" WHERE ");
        int counter = 0;
        for (DocumentCondition documentCondition : documentQuery.getConditions()) {
            Document document = documentCondition.getDocument();
            if (counter > 0) {
                query.append(" AND ");
            }

            query.append(document.getName())
                    .append(' ')
                    .append(document.getValue().get(String.class));
            counter++;
        }
        return query.toString();
    }


}
