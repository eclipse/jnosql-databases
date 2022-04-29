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
package org.eclipse.jnosql.communication.mongodb.document.type;

import jakarta.nosql.metamodel.NumberAttribute;
import jakarta.nosql.metamodel.StringAttribute;

public class Person {
    
    public static volatile StringAttribute<Person> NAME = new StringAttribute<Person>() {
        
        @Override
        public Class<Person> getType() {
            return Person.class;
        }

        @Override
        public Class<String> getAttributeType() {
            return String.class;
        }

        @Override
        public String getName() {
            return "name";
        }
        
    };
    
    public static volatile StringAttribute<Person> CITY = new StringAttribute<Person>() {
        
        @Override
        public Class<Person> getType() {
            return Person.class;
        }

        @Override
        public Class<String> getAttributeType() {
            return String.class;
        }

        @Override
        public String getName() {
            return "city";
        }
        
    };
    
    public static volatile NumberAttribute<Person, Integer> AGE = new NumberAttribute<Person, Integer>() {
        
        @Override
        public Class<Person> getType() {
            return Person.class;
        }

        @Override
        public Class<Integer> getAttributeType() {
            return Integer.class;
        }

        @Override
        public String getName() {
            return "age";
        }
        
    };

    private String id;

    private String city;

    private String name;

    private Integer age;

    public void setId(String id) {
        this.id = id;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getId() {
        return id;
    }

    public String getCity() {
        return city;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
    
}
