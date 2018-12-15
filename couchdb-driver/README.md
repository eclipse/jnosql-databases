![CouchDB Project](http://couchdb.apache.org/image/logo@2x.png)



**Apache CouchDB** is open source database software that focuses on ease of use and having a scalable architecture. It has a document-oriented NoSQL database architecture and is implemented in the concurrency-oriented language Erlang; it uses JSON to store data, JavaScript as its query language using MapReduce, and HTTP for an API.[1]


### How To test

Once this a communication layer to CouchDB, we're using integration test from [testcontainers](https://www.testcontainers.org/), so you need to install Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)

1. Install docker: https://www.docker.com/
1. Execute the test `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
