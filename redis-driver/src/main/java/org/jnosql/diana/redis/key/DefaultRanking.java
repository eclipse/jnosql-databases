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

package org.jnosql.diana.redis.key;

import java.util.Objects;

class DefaultRanking implements Ranking {

    private final Number point;

    private final String member;

    DefaultRanking(String member, Number point) {
        this.point = Objects.requireNonNull(point, "point is required");
        this.member = Objects.requireNonNull(member, "member is required");
    }

    @Override
    public Number getPoints() {
        return point;
    }

    @Override
    public String getMember() {
        return member;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Ranking)) {
            return false;
        }
        Ranking that = (Ranking) o;
        return Objects.equals(point, that.getPoints()) &&
                Objects.equals(member, that.getMember());
    }

    @Override
    public int hashCode() {
        return Objects.hash(point, member);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultRanking{");
        sb.append("point=").append(point);
        sb.append(", member='").append(member).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
