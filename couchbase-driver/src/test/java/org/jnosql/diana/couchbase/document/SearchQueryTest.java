/*
 *  Copyright (c) 2017 Otávio Santana and others
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Apache License v2.0 which accompanies this distribution.
 *   The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *   and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 *   You may elect to redistribute this code under either of these licenses.
 *
 *   Contributors:
 *
 *   Otavio Santana
 */
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.jnosql.diana.couchbase.CouchbaseUtil;
import org.jnosql.diana.couchbase.configuration.CouchbaseDocumentTcConfiguration;
import org.jnosql.diana.couchbase.configuration.CouchbaseKeyValueTcConfiguration;
import org.jnosql.diana.couchbase.keyvalue.CouchbaseKeyValueConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchQueryTest {

    private static CouchbaseDocumentConfiguration configuration;
    private CouchbaseDocumentCollectionManager entityManager;

    {
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManager = managerFactory.get(CouchbaseUtil.BUCKET_NAME);
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.remove("city:salvador");
        keyValueEntityManager.remove("city:sao_paulo");
        keyValueEntityManager.remove("city:rio_janeiro");
        keyValueEntityManager.remove("city:manaus");
    }

    @BeforeAll
    public static void beforeClass() throws InterruptedException {
        configuration = CouchbaseDocumentTcConfiguration.getTcConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        CouchbaseDocumentCollectionManager entityManager = managerFactory.get(CouchbaseUtil.BUCKET_NAME);

        DocumentEntity salvador = DocumentEntity.of("city", asList(Document.of("_id", "salvador")
                , Document.of("name", "Salvador"),
                Document.of("description", "Founded by the Portuguese in 1549 as the first capital" +
                        " of Brazil, Salvador is" +
                        " one of the oldest colonial cities in the Americas.")));
        DocumentEntity saoPaulo = DocumentEntity.of("city", asList(Document.of("_id", "sao_paulo")
                , Document.of("name", "São Paulo"), Document.of("description", "São Paulo, Brazil’s vibrant " +
                        "financial center, is among the world's most populous cities, with numerous cultural institutions" +
                        " and a rich architectural tradition. ")));
        DocumentEntity rioJaneiro = DocumentEntity.of("city", asList(Document.of("_id", "rio_janeiro")
                , Document.of("name", "Rio de Janeiro"), Document.of("description", "Rio de Janeiro " +
                        "is a huge seaside city in Brazil, famed for its Copacabana" +
                        " and Ipanema beaches, 38m Christ the Redeemer statue atop Mount" +
                        " Corcovado and for Sugarloaf Mountain, a granite peak with cable" +
                        " cars to its summit. ")));
        DocumentEntity manaus = DocumentEntity.of("city", asList(Document.of("_id", "manaus")
                , Document.of("name", "Manaus"), Document.of("description", "Manaus, on the banks " +
                        "of the Negro River in northwestern Brazil, is the capital of the vast state of Amazonas.")));

        entityManager.insert(Arrays.asList(salvador, saoPaulo, rioJaneiro, manaus));
        Thread.sleep(2_000L);

    }


    @Test
    public void shouldSearchElement() {
        MatchQuery match = SearchQuery.match("Financial");
        SearchQuery query = new SearchQuery("index-diana", match);
        List<DocumentEntity> entities = entityManager.search(query).collect(Collectors.toList());
        assertEquals(1, entities.size());
        assertEquals(Document.of("name", "São Paulo"), entities.get(0).find("name").get());
    }

    @Test
    public void shouldSearchElement2() {
        MatchQuery match = SearchQuery.match("Brazil");
        SearchQuery query = new SearchQuery("index-diana", match);
        List<DocumentEntity> entities = entityManager.search(query).collect(Collectors.toList());
        assertEquals(3, entities.size());
        List<String> result = entities.stream()
                .flatMap(e -> e.getDocuments().stream())
                .filter(d -> "name".equals(d.getName()))
                .map(d -> d.get(String.class)).collect(Collectors.toList());

        assertThat(result, containsInAnyOrder("Salvador", "Rio de Janeiro", "Manaus"));
    }

    @Test
    public void shouldSearchElement3() {
        MatchQuery match = SearchQuery.match("Salvador").field("name");
        SearchQuery query = new SearchQuery("index-diana", match);
        List<DocumentEntity> entities = entityManager.search(query).collect(Collectors.toList());
        assertEquals(1, entities.size());
        List<String> result = entities.stream()
                .flatMap(e -> e.getDocuments().stream())
                .filter(d -> "name".equals(d.getName()))
                .map(d -> d.get(String.class)).collect(Collectors.toList());

        assertThat(result, containsInAnyOrder("Salvador"));
    }

}
