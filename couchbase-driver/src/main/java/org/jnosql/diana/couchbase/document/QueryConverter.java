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
package org.jnosql.diana.couchbase.document;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Delete;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.MutateLimitPath;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.List;

import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.x;

final class QueryConverter {


    static QueryConverterResult select(DocumentQuery query, String bucket) {
        JsonObject params = JsonObject.create();
        String[] documents = query.getDocuments().stream().toArray(size -> new String[size]);
        Statement statement = Select.select(documents).from(i(bucket))
                .where(getCondition(query.getCondition(), params));
        return new QueryConverterResult(params, statement);
    }

    static QueryConverterResult delete(DocumentQuery query, String bucket) {
        JsonObject params = JsonObject.create();
        Expression condition = getCondition(query.getCondition(), params);
        MutateLimitPath statement = Delete.deleteFrom(bucket).where(condition);
        return new QueryConverterResult(params, statement);
    }

    private static Expression getCondition(DocumentCondition condition, JsonObject params) {
        Document document = condition.getDocument();
        switch (condition.getCondition()) {
            case EQUALS:
                params.put(document.getName(), document.get());
                return x(document.getName()).eq(x('$' + document.getName()));
            case LESSER_THAN:
                params.put(document.getName(), document.get());
                return x(document.getName()).lt(x('$' + document.getName()));
            case LESSER_EQUALS_THAN:
                params.put(document.getName(), document.get());
                return x(document.getName()).lte(x('$' + document.getName()));
            case GREATER_THAN:
                params.put(document.getName(), document.get());
                return x(document.getName()).gt(x('$' + document.getName()));
            case GREATER_EQUALS_THAN:
                params.put(document.getName(), document.get());
                return x(document.getName()).gte(x('$' + document.getName()));
            case LIKE:
                params.put(document.getName(), document.get());
                return x(document.getName()).like(x('$' + document.getName()));
            case IN:
                params.put(document.getName(), document.get());
                return x(document.getName()).like(x('$' + document.getName()));
            case AND:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, params))
                        .reduce((d1, d2) -> d1.and(d2))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));


            case OR:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, params))
                        .reduce((d1, d2) -> d1.or(d2))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));
            case NOT:
                DocumentCondition dc = document.get(DocumentCondition.class);
                return getCondition(dc, params).not();
            default:
                throw new IllegalStateException("This condition is not supported at coubhbase: " + condition.getCondition());
        }
    }

    static class QueryConverterResult {

        private final JsonObject params;

        private final Statement statement;

        public QueryConverterResult(JsonObject params, Statement statement) {
            this.params = params;
            this.statement = statement;
        }

        public JsonObject getParams() {
            return params;
        }

        public Statement getStatement() {
            return statement;
        }
    }


}
