package org.jnosql.diana.couchdb.document.test;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties({"id", "revision"})
public class Sofa {

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_rev")
    private String revision;

    private String color;

    public void setId(String s) {
        id = s;
    }

    public String getId() {
        return id;
    }

    public String getRevision() {
        return revision;
    }

    public void setColor(String s) {
        color = s;
    }

    public String getColor() {
        return color;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Sofa{");
        sb.append("id='").append(id).append('\'');
        sb.append(", revision='").append(revision).append('\'');
        sb.append(", color='").append(color).append('\'');
        sb.append('}');
        return sb.toString();
    }
}