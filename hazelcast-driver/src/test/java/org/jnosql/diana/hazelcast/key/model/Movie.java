package org.jnosql.diana.hazelcast.key.model;

import java.io.Serializable;

public class Movie implements Serializable {

    private String name;

    private Integer year;

    private boolean active;


    public Movie(String name, Integer year, boolean active) {
        this.name = name;
        this.year = year;
        this.active = active;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Movie{");
        sb.append("name='").append(name).append('\'');
        sb.append(", year=").append(year);
        sb.append(", active=").append(active);
        sb.append('}');
        return sb.toString();
    }
}
