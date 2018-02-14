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

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.orientdb.document.QueryOSQLConverter.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

final class QueryOSQLFactory {

    public static final String LIVE = "LIVE ";

    private QueryOSQLFactory() {
    }

    static QueryResult to(DocumentQuery documentQuery) {
        Query query = QueryOSQLConverter.select(documentQuery);

        return new QueryResult(new OSQLSynchQuery<ODocument>(query.getQuery()) {
        }, query.getParams());
    }

    static OSQLSynchQuery<ODocument> parse(String query) {
        return new OSQLSynchQuery<ODocument>(query) {
        };
    }

    static QueryResult toAsync(DocumentQuery documentQuery, Consumer<List<ODocument>> callBack) {
        Query query = QueryOSQLConverter.select(documentQuery);
        return new QueryResult(new OSQLAsynchQuery<>(query.getQuery(), new OCommandResultListener() {
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

    static QueryResult toLive(DocumentQuery documentQuery, OrientDBLiveCallback callbacks) {
        Query query = QueryOSQLConverter.select(documentQuery);
        OLiveQuery<ODocument> liveQuery = new OLiveQuery<>(LIVE + query.getQuery(),
                new LiveQueryLIstener(callbacks));
        return new QueryResult(liveQuery, query.getParams());

    }

    static QueryResult toAsync(String query, Consumer<List<ODocument>> callBack, Object... params) {
        return new QueryResult(new OSQLAsynchQuery<>(query, new OCommandResultListener() {
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

        Object[] getParams() {
            return params.toArray(new Object[params.size()]);
        }
    }
}
