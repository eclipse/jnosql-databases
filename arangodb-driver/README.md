![Arangodb Project](https://github.com/JNOSQL/jnosql-site/blob/master/assets/img/logos/arangodb.png)


**Couchbase**: Couchbase Server, originally known as Membase, is an open-source, distributed multi-model NoSQL document-oriented database software package that is optimized for interactive applications.


### How To test

Once this a communication layer to Couchbase, we're using integration test, so you need to install ArangoDB. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Install docker: https://www.docker.com/
1. https://hub.docker.com/r/couchbase/server/
1. Run docker command
1. `docker run -d --name couchbase-instance -p 8091-8094:8091-8094 -p 11210:11210 couchbase`
1. Follow the instructions: https://hub.docker.com/r/couchbase/server/
1. Execute the test `mvn clean install`
