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
