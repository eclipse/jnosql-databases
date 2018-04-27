
![MongoDB Project](https://jnosql.github.io/img/logos/mongodb.png)


**Mongodb**: MongoDB is a free and open-source cross-platform document-oriented database program. Classified as a NoSQL database program, MongoDB uses JSON-like documents with schemas.


### How To Install

1. Execute the maven install `mvn clean install`


### Install without testing

`sudo docker run -d -p 8080:8080 -p 38888:38888 ravendb/ravendb`

If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
