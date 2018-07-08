package org.jnosql.diana.couchdb.document.test;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
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
        SofaRepository repository = new SofaRepository(db);
        repository.add(sofa);
    }


}
