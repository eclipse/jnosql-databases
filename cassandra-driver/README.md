
![Cassandra Project](https://jnosql.github.io/jnosql-site/img/logos/cassandra.png)


**Cassandra**: Apache Cassandra is a free and open-source distributed database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure.

### How To install

On Cassandra, the integration test is made with cassandra-unit, so just run the tests and install with maven command:
1. Execute the test `mvn clean install`

### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
