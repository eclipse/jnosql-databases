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
 */
package org.jnosql.diana.couchbase.document;

import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.Sort;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;

final class StatementFactory {

    private StatementFactory() {
    }

    static Statement create(String bucket, String[] documents,
                            int firstResult,
                            int maxResult, Sort[] sorts) {

        boolean hasFistResult = firstResult > 0;
        boolean hasMaxResult = maxResult > 0;

        if (hasFistResult && hasMaxResult) {
            return select(documents)
                    .from(i(bucket))
                    .orderBy(sorts)
                    .limit(maxResult)
                    .offset(firstResult);
        } else if (hasFistResult) {
            return select(documents).from(i(bucket)).orderBy(sorts).offset(firstResult);
        } else if (hasMaxResult) {
            return select(documents).from(i(bucket)).orderBy(sorts).limit(maxResult);
        }
        return select(documents).from(i(bucket)).orderBy(sorts);
    }

    static Statement create(String bucket, String[] documents,
                            int firstResult,
                            int maxResult,
                            Sort[] sorts,
                            Expression condition) {

        boolean hasFistResult = firstResult > 0;
        boolean hasMaxResult = maxResult > 0;

        if (hasFistResult && hasMaxResult) {
            return select(documents).from(i(bucket))
                    .where(condition)
                    .orderBy(sorts)
                    .limit(maxResult)
                    .offset(firstResult);

        } else if (hasFistResult) {
            return select(documents).from(i(bucket))
                    .where(condition)
                    .orderBy(sorts)
                    .offset(firstResult);
        } else if (hasMaxResult) {
            return select(documents).from(i(bucket)).where(condition)
                    .orderBy(sorts)
                    .limit(maxResult);
        }
        return select(documents).from(i(bucket)).where(condition).orderBy(sorts);

    }

}
