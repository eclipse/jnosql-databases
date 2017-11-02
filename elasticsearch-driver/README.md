![Elasticsearch Project](https://jnosql.github.io/jnosql-site/img/logos/elastic.svg)


**Elasticsearch**: Elasticsearch is a search engine based on Lucene. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents. Elasticsearch is developed in Java and is released as open source under the terms of the Apache License. Elasticsearch is the most popular enterprise search engine followed by Apache Solr, also based on Lucene.


### How To Install

1. Download the code: https://www.elastic.co/downloads/elasticsearch
1. Follow the install and run steps: https://www.elastic.co/downloads/elasticsearch
1. Execute the maven install `mvn clean install`


### Install without testing


If you won't run the tests the database is not required, so just run the maven skipping the tests.

1. Execute the test `mvn clean install -DskipTests`
