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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.orientdb.document.QueryOSQLConverter.Query;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

final class QueryOSQLFactory {

    public static final String LIVE = "LIVE ";

    private QueryOSQLFactory() {
    }

    static QueryResult to(DocumentQuery documentQuery) {
        Query query = QueryOSQLConverter.select(documentQuery);
        return new QueryResult(query.getQuery(), query.getParams(), query.getIds());
    }

    static OSQLSynchQuery<ODocument> parse(String query) {
        return new OSQLSynchQuery<ODocument>(query) {
        };
    }

    static QueryResult toAsync(DocumentQuery documentQuery, Consumer<List<ODocument>> callBack) {
        Query query = QueryOSQLConverter.select(documentQuery);
        return null;
    /*    return new QueryResult(new OSQLAsynchQuery<>(query.getQuery(), new OCommandResultListener() {
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
        }), query.getParams());*/
    }

    static QueryResult toLive(DocumentQuery documentQuery, OrientDBLiveCallback callbacks) {
        Query query = QueryOSQLConverter.select(documentQuery);
        return new QueryResult(LIVE + query.getQuery(), query.getParams(), Collections.emptyList());
    }

    static QueryResult toAsync(String query, Consumer<List<ODocument>> callBack, Object... params) {
        return null;
        /*return new QueryResult(new OSQLAsynchQuery<>(query, new OCommandResultListener() {
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
        }), asList(params));*/
    }

    static class QueryResult {

        private final String query;
        private final List<Object> params;
        private final List<ORecordId> ids;

        QueryResult(String query, List<Object> params, List<ORecordId> ids) {
            this.query = query;
            this.params = params;
            this.ids = ids;
        }

        String getQuery() {
            return query;
        }

        Object[] getParams() {
            return params.toArray(new Object[params.size()]);
        }

        public List<ORecordId> getIds() {
            return ids;
        }

        public boolean isRunQuery() {
            return ids.isEmpty() || (!ids.isEmpty() && !params.isEmpty());
        }

        public boolean isLoad() {
            return !ids.isEmpty();
        }
    }
}
