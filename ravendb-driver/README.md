![RavenDB Project](https://ravendb.net/Content/images/logo/ravendb_logo_small.jpg)



**RavenDB**: The Original Fully Transactional Open Source NoSQL Document Database. Easy to use, rapidly scalable, offers high availability, and takes your Business into the Next Generation of Data Performance.


### How To test

Once this a communication layer to Couchbase, we're using integration test, so you need to install Couchbase. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Install docker: https://www.docker.com/
1. https://hub.docker.com/r/ravendb/ravendb/
1. Run docker command
1. `sudo docker run -d -p 8080:8080 -p 38888:38888 ravendb/ravendb`
1. Go to: http://localhost:8080/
1. Create a database `database`
1. Execute the test `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
