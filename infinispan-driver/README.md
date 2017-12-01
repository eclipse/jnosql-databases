![Infinista Project](https://jnosql.github.io/img/logos/infinispan.svg)


**Infinispan**:Infinispan is a distributed in-memory key/value data store with optional schema, available under the Apache License 2.0.

* Available as an embedded Java library or as a language-independent service accessed remotely over a variety of protocols (Hot Rod, REST, Memcached and WebSockets)
* Use it as a cache or a data grid.
* Advanced functionality such as transactions, events, querying, distributed processing, off-heap and geographical failover.
* Monitor and manage it through JMX, a CLI and a web-based console.
* Integrates with JPA, JCache, Spring, Spark and many more.
* Works on AWS, Azure, Google Cloud and OpenShift.


### How To Install

1. Execute the maven install `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
