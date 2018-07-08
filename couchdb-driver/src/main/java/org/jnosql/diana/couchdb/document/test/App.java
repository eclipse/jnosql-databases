package org.jnosql.diana.couchdb.document.test;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import java.net.MalformedURLException;

public class App {

    public static void main(String[] args) throws MalformedURLException {
        HttpClient httpClient = new StdHttpClient.Builder()
                .url("http://localhost:5984")
                .build();

        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        CouchDbConnector db = new StdCouchDbConnector("person", dbInstance);
        db.createDatabaseIfNotExists();
        Sofa sofa = new Sofa();
        sofa.setColor("blue");
        ViewQuery viewQuery = new ViewQuery().queryParam("color", "blue");
        ViewResult rows = db.queryView(viewQuery);
        System.out.println(rows);
    }


}
