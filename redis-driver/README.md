
![Redis Project](https://jnosql.github.io/img/logos/redis.png)



**Redis**: Redis is a software project that implements data structure servers. It is open-source, networked, in-memory, and stores keys with optional durability.

### How To Install

Once this is a communication layer to Redis, we're using integration test, so you need to install Redis. The recommended way is using Docker.

![Docker](https://www.docker.com/sites/default/files/horizontal_large.png)


1. Execute the maven install `mvn clean install`


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
