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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.List;
import java.util.function.Consumer;

final class OSQLQueryFactory {

    private OSQLQueryFactory() {
    }

    public static QueryResult to(DocumentQuery documentQuery) {
        Query query = getQuery(documentQuery);

        return new QueryResult(new OSQLSynchQuery<ODocument>(query.getQuery()) {
        }, query.getParams());
    }

    public static QueryResult toDelete(DocumentQuery documentQuery) {
        Query query = getQuery(documentQuery);

        return new QueryResult(new OSQLSynchQuery<ODocument>(query.getQuery()) {
        }, query.getParams());
    }

    public static OSQLAsynchQuery<ODocument> toAsync(DocumentQuery documentQuery, Consumer<Void> callBack) {
        Query query = getQuery(documentQuery);
        return new OSQLAsynchQuery<ODocument>(query.getQuery(), new OCommandResultListener() {
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

    private static Query getQuery(DocumentQuery documentQuery) {
        return getQuery(documentQuery, false);
    }

    private static Query getQuery(DocumentQuery documentQuery, boolean isDelete) {
        StringBuilder query = new StringBuilder();
        List<Object> params = new java.util.ArrayList<>();
        if (isDelete) {
            query.append("DELETE FROM ");
        } else {
            query.append("SELECT FROM ");
        }
        query.append(documentQuery.getCollection()).append(" WHERE ");
        int counter = 0;
        for (DocumentCondition documentCondition : documentQuery.getConditions()) {
            Document document = documentCondition.getDocument();
            Condition condition = documentCondition.getCondition();
            if (counter > 0) {
                query.append(" AND ");
            }
            query.append(document.getName())
                    .append(' ')
                    .append(toCondition(condition))
                    .append(' ')
                    .append('?');
            counter++;
            params.add(document.get());
        }
        return new Query(query.toString(), params);
    }


    private static String toCondition(Condition condition) {
        switch (condition) {
            case AND:
                return "AND";
            case EQUALS:
                return "=";
            case GREATER_EQUALS_THAN:
                return ">=";
            case GREATER_THAN:
                return ">";
            case IN:
                return "IN";
            case LESSER_EQUALS_THAN:
                return "<=";
            case LESSER_THAN:
                return "<";
            case LIKE:
                return "LIKE";
            case NOT:
                return "NOT";
            case OR:
                return "OR";
            default:
                throw new IllegalArgumentException("Orient DB has not support to the condition " + condition);

        }
    }

    static class Query {
        private final String query;
        private final List<Object> params;

        public Query(String query, List<Object> params) {
            this.query = query;
            this.params = params;
        }

        public String getQuery() {
            return query;
        }

        public List<Object> getParams() {
            return params;
        }
    }

    static class QueryResult {

        private final OSQLQuery<ODocument> query;
        private final List<Object> params;

        public QueryResult(OSQLQuery<ODocument> query, List<Object> params) {
            this.query = query;
            this.params = params;
        }

        public OSQLQuery<ODocument> getQuery() {
            return query;
        }

        public Object getParams() {
            return params.toArray(new Object[params.size()]);
        }
    }
}
