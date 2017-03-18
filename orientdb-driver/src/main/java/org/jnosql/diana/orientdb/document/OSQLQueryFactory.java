/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

final class OSQLQueryFactory {

    public static final String LIVE = "live ";

    private OSQLQueryFactory() {
    }

    static QueryResult to(DocumentQuery documentQuery) {
        Query query = getQuery(documentQuery);

        return new QueryResult(new OSQLSynchQuery<ODocument>(query.getQuery()) {
        }, query.getParams());
    }

    static OSQLSynchQuery<ODocument> parse(String query) {
        return new OSQLSynchQuery<ODocument>(query) {
        };
    }

    static OLiveQuery<ODocument> parseOlive(String query, Consumer<DocumentEntity> callBack) {
        return new OLiveQuery<>(query, new LiveQueryLIstener(callBack));
    }

    static QueryResult toDelete(DocumentQuery documentQuery) {
        Query query = getQuery(documentQuery);

        return new QueryResult(new OSQLSynchQuery<ODocument>(query.getQuery()) {
        }, query.getParams());
    }

    static QueryResult toAsync(DocumentQuery documentQuery, Consumer<List<ODocument>> callBack) {
        Query query = getQuery(documentQuery);
        return new QueryResult(new OSQLAsynchQuery<ODocument>(query.getQuery(), new OCommandResultListener() {
            private List<ODocument> documents = new ArrayList<>();

            @Override
            public boolean result(Object iRecord) {
                ODocument document = (ODocument) iRecord;
                documents.add(document);
                return true;
            }

            @Override
            public void end() {
                callBack.accept(documents);
            }

            @Override
            public Object getResult() {
                return null;
            }
        }), query.getParams());
    }

    static QueryResult toLive(DocumentQuery documentQuery, Consumer<DocumentEntity> callBack) {
        Query query = getQuery(documentQuery);
        OLiveQuery<ODocument> liveQuery = new OLiveQuery<>(LIVE + query.getQuery(), new LiveQueryLIstener(callBack));
        return new QueryResult(liveQuery, query.getParams());

    }

    static QueryResult toAsync(String query, Consumer<List<ODocument>> callBack, Object... params) {
        return new QueryResult(new OSQLAsynchQuery<ODocument>(query, new OCommandResultListener() {
            private List<ODocument> documents = new ArrayList<>();

            @Override
            public boolean result(Object iRecord) {
                ODocument document = (ODocument) iRecord;
                documents.add(document);
                return true;
            }

            @Override
            public void end() {
                callBack.accept(documents);
            }

            @Override
            public Object getResult() {
                return null;
            }
        }), asList(params));
    }


    private static Query getQuery(DocumentQuery documentQuery) {
        StringBuilder query = new StringBuilder();
        List<Object> params = new java.util.ArrayList<>();
        query.append("SELECT FROM ");
        query.append(documentQuery.getCollection());
        int counter = 0;
        if (documentQuery.getCondition().isPresent()) {
            query.append(" WHERE ");
            counter = addCondition(documentQuery.getCondition().get(), counter, query, params);
        }
        return new Query(query.toString(), params);
    }

    private static int addCondition(DocumentCondition documentCondition, int counter, StringBuilder query, List<Object> params) {
        return addCondition(documentCondition, counter, query, params, "AND");
    }

    private static int addCondition(DocumentCondition documentCondition, int counter, StringBuilder query, List<Object> params, String connector) {
        Document document = documentCondition.getDocument();
        Condition condition = documentCondition.getCondition();
        int aux = counter;
        if (Condition.AND.equals(condition) || Condition.OR.equals(condition) || Condition.NOT.equals(condition)) {
            List<DocumentCondition> documentConditions = document.get(new TypeReference<List<DocumentCondition>>() {
            });

            for (DocumentCondition dc : documentConditions) {
                aux = addCondition(dc, aux, query, params, toCondition(condition));
            }
            return aux;

        }
        if (counter > 0) {
            query.append(' ').append(connector).append(' ');
        }
        query.append(document.getName())
                .append(' ')
                .append(toCondition(condition))
                .append(' ')
                .append('?');
        params.add(document.get());
        return aux + 1;
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

        Query(String query, List<Object> params) {
            this.query = query;
            this.params = params;
        }

        String getQuery() {
            return query;
        }

        List<Object> getParams() {
            return params;
        }
    }

    static class QueryResult {

        private final OSQLQuery<ODocument> query;
        private final List<Object> params;

        QueryResult(OSQLQuery<ODocument> query, List<Object> params) {
            this.query = query;
            this.params = params;
        }

        OSQLQuery<ODocument> getQuery() {
            return query;
        }

        Object getParams() {
            return params.toArray(new Object[params.size()]);
        }
    }
}
