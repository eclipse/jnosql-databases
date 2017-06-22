
![Riak Project](https://github.com/JNOSQL/jnosql-site/blob/master/assets/img/logos/riak.png)


**Riak**: Riak (pronounced "ree-ack" ) is a distributed NoSQL key-value data store that offers high availability, fault tolerance, operational simplicity, and scalability. In addition to the open-source version, it comes in a supported enterprise version and a cloud storage version. Riak implements the principles from Amazon's Dynamo paper with heavy influence from the CAP Theorem. Written in Erlang, Riak has fault tolerance data replication and automatic data distribution across the cluster for performance and resilience.


### How To Install

Once this a communication layer to Riak, we're using integration test, so you need to install Riak. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Install docker: https://www.docker.com/
1. https://hub.docker.com/r/basho/riak-kv/
1. Run docker command
1. `docker run --name riak-instance -d -p 8087:8087 -p 8098:8098 basho/riak-ts`
1. Execute the maven install `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
