![Couchbase Project](https://jnosql.github.io/img/logos/couchbase.svg)



**Couchbase**: Couchbase Server, originally known as Membase, is an open-source, distributed multi-model NoSQL document-oriented database software package that is optimized for interactive applications.


### How To test

Once this a communication layer to Couchbase, we're using integration test, so you need to install Couchbase. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Install docker: https://www.docker.com/
1. https://hub.docker.com/r/couchbase/server/
1. Run docker command
1. `docker run -d --name couchbase-instance -p 8091-8094:8091-8094 -p 11210:11210 couchbase`
1. Follow the instructions: https://hub.docker.com/r/couchbase/server/
1. Create a **jnosql** bucket name
1. create an index running the query `CREATE PRIMARY INDEX index_jnosql on jnosql;`
1. Follow the instructions: https://developer.couchbase.com/documentation/server/current/fts/full-text-intro.html to create a index-diana full text index
1. Execute the test `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`

### Adding dependencies

If you are not using a Java EE application server, you must add the following dependencies:

Maven
```xml
<dependency>
    <groupId>org.eclipse</groupId>
    <artifactId>yasson</artifactId>
    <version>1.0</version>
</dependency>

<dependency>
    <groupId>org.glassfish</groupId>
    <artifactId>javax.json</artifactId>
    <version>1.1</version>
</dependency>
```
Gradle
```groovy
compile('org.eclipse:yasson:1.0')
compile('org.glassfish:javax.json:1.1')
```
