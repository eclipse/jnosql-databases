package org.jnosql.diana.arangodb.document;

public class Person {

    private String name = "Ada";

    private String city = "London";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
