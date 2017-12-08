package org.jnosql.diana.hazelcast.key.model;

import java.io.Serializable;

public class Movie implements Serializable {

    private String name;

    private String description;

    private Integer year;

    private boolean active;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
        sb.append(", description='").append(description).append('\'');
        sb.append(", year=").append(year);
        sb.append(", active=").append(active);
        sb.append('}');
        return sb.toString();
    }
}
