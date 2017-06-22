
![Riak Project](https://github.com/JNOSQL/jnosql-site/blob/master/assets/img/logos/riak.png)


**Arangodb**: ArangoDB is a native multi-model database with flexible data models for documents, graphs, and key-values. Build high performance applications using a convenient SQL-like query language or JavaScript extensions.


### How To Install

Once this a communication layer to Arango, we're using integration test, so you need to install ArangoDB. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Install docker: https://www.docker.com/
1. https://hub.docker.com/r/arangodb/arangodb/
1. Run docker command
1. `docker run -e ARANGO_NO_AUTH=1 -d --name arangodb-instance -p 8529:8529 -d arangodb/arangodb`
1. Execute the maven install `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
