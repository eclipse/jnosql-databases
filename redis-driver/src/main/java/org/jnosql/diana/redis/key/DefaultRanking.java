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
