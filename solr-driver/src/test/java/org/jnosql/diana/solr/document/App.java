package org.jnosql.diana.solr.document;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

public class App {

    public static void main(String[] args) throws IOException, SolrServerException {
        String urlString = "http://localhost:8983/solr/database";
        HttpSolrClient solr = new HttpSolrClient.Builder(urlString).build();
        solr.setParser(new XMLResponseParser());

        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "123456");
        document.addField("name", "Kenmore Dishwasher");
        document.addField("price", "599.99");
        solr.add(document);
        solr.commit();
    }
}
