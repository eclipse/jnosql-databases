/*
 *  Copyright (c) 2022 Otávio Santana and others
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
 *   Alessandro Moscatelli
 */
package org.eclipse.jnosql.communication.mongodb.document;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.empty;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.where;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Aggregates.count;
import static com.mongodb.client.model.Aggregates.group;
import jakarta.nosql.criteria.BinaryPredicate;
import jakarta.nosql.criteria.CompositionPredicate;
import jakarta.nosql.criteria.CriteriaFunction;
import jakarta.nosql.criteria.DisjunctionPredicate;
import jakarta.nosql.criteria.Expression;
import jakarta.nosql.criteria.ExpressionFunction;
import jakarta.nosql.criteria.NegationPredicate;
import jakarta.nosql.criteria.Path;
import jakarta.nosql.criteria.PathFunction;
import jakarta.nosql.criteria.Predicate;
import jakarta.nosql.criteria.Root;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.conversions.Bson;

public class CriteriaQueryUtils {

    private CriteriaQueryUtils() {
    }

    public static String join(String... values) {
        return String.join(".", values);
    }

    public static String unfold(Path path) {
        Path tmp = path;
        Deque<String> attributes = new ArrayDeque();
        while (Objects.nonNull(tmp) && !(tmp instanceof Root)) {
            attributes.add(
                    tmp.getAttribute().getName()
            );
            tmp = tmp.getParent();
        }
        return join(
                attributes.stream().filter(
                        value -> !Objects.equals(
                                0,
                                value.trim().length()
                        )
                ).toArray(String[]::new)
        );
    }

    public static String unfold(Expression expression) {
        return join(
                Arrays.asList(
                        unfold(
                                expression.getPath()
                        ),
                        expression.getAttribute().getName()
                ).stream().filter(
                        value -> !Objects.equals(
                                0,
                                value.trim().length()
                        )
                ).toArray(
                        String[]::new
                )
        );
    }

    public static Bson computeRestriction(Predicate predicate) {
        Bson result = empty();
        if (predicate instanceof CompositionPredicate) {
            Collection<Predicate> restrictions = CompositionPredicate.class.cast(predicate).getPredicates();
            if (!restrictions.isEmpty()) {
                Function<Bson[], Bson> function = predicate instanceof DisjunctionPredicate
                        ? Filters::or
                        : Filters::and;
                result = function.apply(
                        restrictions.stream().map(
                                restriction -> computeRestriction(restriction)
                        ).collect(
                                Collectors.toList()
                        ).toArray(Bson[]::new)
                );
            }
        } else if (predicate instanceof NegationPredicate) {
            result = not(
                    computeRestriction(
                            NegationPredicate.class.cast(predicate).getPredicate()
                    )
            );
        } else if (predicate instanceof BinaryPredicate) {
            BinaryPredicate cast = BinaryPredicate.class.cast(predicate);
            String lhs = unfold(
                    cast.getLeft()
            );
            Object rhs = cast.getRight();
            if (rhs instanceof Expression) {
                // Filter.expr from mongodb driver cannot be used as a right hand side when comparing fields
                // hence I am forced to use Filder.where and manually build the string
                // Change this if future driver versions will improve
                String operator = null;
                switch (cast.getOperator()) {
                    case EQUAL:
                        operator = "==";
                        break;
                    case GREATER_THAN:
                        operator = ">";
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        operator = ">=";
                        break;
                    case LESS_THAN:
                        operator = "<";
                        break;
                    case LESS_THAN_OR_EQUAL:
                        operator = "<=";
                        break;
                    default:
                        break;
                }
                if (Objects.nonNull(operator)) {
                    result = where(
                            String.format(
                                    "this.%s %s this.%s",
                                    lhs,
                                    operator,
                                    unfold(
                                            Expression.class.cast(cast.getRight())
                                    )
                            )
                    );
                }
            } else {
                switch (cast.getOperator()) {
                    case EQUAL:
                        result = eq(
                                lhs,
                                rhs
                        );
                        break;
                    case GREATER_THAN:
                        result = gt(
                                lhs,
                                rhs
                        );
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        result = gte(
                                lhs,
                                rhs
                        );
                        break;
                    case LESS_THAN:
                        result = lt(
                                lhs,
                                rhs
                        );
                        break;
                    case LESS_THAN_OR_EQUAL:
                        result = lte(
                                lhs,
                                rhs
                        );
                        break;
                    case IN:
                        result = in(
                                lhs,
                                rhs
                        );
                        break;
                    case LIKE:
                        result = regex(
                                lhs,
                                "^" + String.class.cast(rhs).replace("%", ".*") + "$"
                        );
                        break;
                    default:
                        break;
                }
            }
        }
        return result;
    }

    public static Bson computeAggregation(CriteriaFunction function) {
        if (function instanceof PathFunction) {
            PathFunction cast = PathFunction.class.cast(function);
            String unfold = unfold(
                    cast.getPath()
            );
            switch (cast.getFunction()) {
                case COUNT:
                    return group(
                            null,
                            Accumulators.sum(
                                    Integer.toString(cast.hashCode()),
                                    1
                            )
                    );
                default:
                    break;
            }
        } else if (function instanceof ExpressionFunction) {
            ExpressionFunction cast = ExpressionFunction.class.cast(function);
            String unfold = unfold(
                    cast.getExpression()
            );
            switch (cast.getFunction()) {
                case SUM:
                    return group(
                            null,
                            Accumulators.sum(
                                    Integer.toString(cast.hashCode()),
                                    "$" + unfold
                            )
                    );
                default:
                    break;
            }
        }

        throw new UnsupportedOperationException();
    }

}
