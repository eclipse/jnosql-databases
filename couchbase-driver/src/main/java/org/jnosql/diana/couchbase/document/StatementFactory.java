package org.jnosql.diana.couchbase.document;

import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;

final class StatementFactory {

    private StatementFactory() {
    }

    static Statement create(String bucket, String[] documents,
                            int firstResult,
                            int maxResult) {

        boolean hasFistResult = firstResult > 0;
        boolean hasMaxResult = maxResult > 0;

        if (hasFistResult && hasMaxResult) {
            return select(documents).from(i(bucket)).limit(maxResult).offset(firstResult);
        } else if (hasFistResult) {
            return select(documents).from(i(bucket)).offset(firstResult);
        } else if (hasMaxResult) {
            return select(documents).from(i(bucket)).limit(maxResult);
        }
        return select(documents).from(i(bucket));
    }

    static Statement create(String bucket, String[] documents,
                            int firstResult,
                            int maxResult,
                            Expression condition) {

        boolean hasFistResult = firstResult > 0;
        boolean hasMaxResult = maxResult > 0;

        if (hasFistResult && hasMaxResult) {
            return select(documents).from(i(bucket)).where(condition)
                    .limit(maxResult).offset(firstResult);
        } else if (hasFistResult) {
            return select(documents).from(i(bucket))
                    .where(condition)
                    .offset(firstResult);
        } else if (hasMaxResult) {
            return select(documents).from(i(bucket)).where(condition).limit(maxResult);
        }
        return select(documents).from(i(bucket)).where(condition);

    }

}
