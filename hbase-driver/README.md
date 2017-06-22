
![Hbase Project](https://github.com/JNOSQL/jnosql-site/blob/master/assets/img/logos/hbase.png)


**Hbase**: HBase is an open source, non-relational, distributed database modeled after Google's BigTable and is written in Java. It is developed as part of Apache Software Foundation's Apache Hadoop project and runs on top of HDFS (Hadoop Distributed File System), providing BigTable-like capabilities for Hadoop. That is, it provides a fault-tolerant way of storing large quantities of sparse data (small amounts of information caught within a large collection of empty or unimportant data, such as finding the 50 largest items in a group of 2 billion records, or finding the non-zero items representing less than 0.1% of a huge collection).


### How To Install

1. Download the code: http://www.apache.org/dyn/closer.cgi/hbase/
1. Follow the install and run steps: https://hbase.apache.org/book.html#quickstart
1. Execute the maven install `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
