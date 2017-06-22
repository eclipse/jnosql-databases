![Arangodb Project](https://github.com/JNOSQL/jnosql-site/blob/master/assets/img/logos/arangodb.png)


**Arangodb**: ArangoDB is a native multi-model database with flexible data models for documents, graphs, and key-values. Build high performance applications using a convenient SQL-like query language or JavaScript extensions.


### How To test

![Arangodb Project](https://www.docker.com/sites/all/themes/docker/assets/images/brand-full.svg)

1. Install docker: https://www.docker.com/
1. https://hub.docker.com/r/arangodb/arangodb/
1. Run docker command
1. `docker run -e ARANGO_NO_AUTH=1 -d --name arangodb-instance -p 8529:8529 -d arangodb/arangodb`
1. Execute the test `mvn clean install`
