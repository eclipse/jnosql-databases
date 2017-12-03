
![Redis Project](https://jnosql.github.io/img/logos/redis.png)



**Redis**: Redis is a software project that implements data structure servers. It is open-source, networked, in-memory, and stores keys with optional durability.

### How To Install

Once this a communication layer to Arango, we're using integration test, so you need to install ArangoDB. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Install docker: https://www.docker.com/
1. https://store.docker.com/images/redis
1. Run docker command
1. `docker run --name redis-instance -p 6379:6379 -d redis`
1. Execute the maven install `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
