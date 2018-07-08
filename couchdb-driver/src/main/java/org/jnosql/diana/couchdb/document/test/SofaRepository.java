package org.jnosql.diana.couchdb.document.test;

import org.ektorp.CouchDbConnector;
import org.ektorp.support.CouchDbRepositorySupport;
import org.ektorp.support.GenerateView;

import java.util.List;

public class SofaRepository extends CouchDbRepositorySupport<Sofa> {

    public SofaRepository(CouchDbConnector db) {
        super(Sofa.class, db);
    }

    @GenerateView
    public List<Sofa> findByTag(String color) {
        return queryView("color", color);
    }

}
