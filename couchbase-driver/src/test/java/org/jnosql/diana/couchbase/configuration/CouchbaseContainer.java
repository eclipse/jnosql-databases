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
package org.jnosql.diana.couchbase;

import static org.jnosql.diana.couchbase.CouchbaseUtil.BUCKET_NAME;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import com.couchbase.client.core.utils.Base64;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.query.N1qlQuery;

public class CouchbaseContainer {

    private static final Logger LOGGER = Logger.getLogger(CouchbaseContainer.class.getName());
    private static final String INDEX_JSON = "index-diana.json";

    private static CouchbaseContainer instance;
    private final GenericContainer couchbase;

    private String user;
    private String password;

    private CouchbaseContainer(String user, String password) {
        this.user = user;
        this.password = password;
        this.couchbase =
                new FixedHostPortGenericContainer("couchbase:latest")
                        .withFixedExposedPort(8091, 8091)
                        .withFixedExposedPort(8092, 8092)
                        .withFixedExposedPort(8093, 8093)
                        .withFixedExposedPort(8094, 8094)
                        .withFixedExposedPort(11210, 11210)
                        .waitingFor(getCompositeWaitStrategy());

        couchbase.start();
        initCluster();
        createBucket();
        createFullTextIndex(couchbase);
    }

    public static CouchbaseContainer start(String user, String password) {
        if (instance == null) {
            instance = new CouchbaseContainer(user, password);
        }
        return instance;
    }

    private void callCouchbaseRestAPI(String url, String content,
            String username, String password, String requestMethod) throws IOException {
        callCouchbaseRestAPI(url, content, username, password, requestMethod
                , "application/x-www-form-urlencoded");
    }

    private void callCouchbaseRestAPI(String url, String content) throws IOException {
        callCouchbaseRestAPI(url, content, null, null, "POST"
                , "application/x-www-form-urlencoded");
    }

    private void callCouchbaseRestAPI(String url, String payload, String username, String password,
            String requestMethod, String contentType) throws IOException {
        HttpURLConnection httpConnection = (HttpURLConnection) ((new URL(url).openConnection()));
        httpConnection.setDoOutput(true);
        httpConnection.setRequestMethod(requestMethod);
        httpConnection.setRequestProperty("Content-Type", contentType);
        if (username != null) {
            String encoded = Base64.encode((username + ":" + password).getBytes("UTF-8"));
            httpConnection.setRequestProperty("Authorization", "Basic " + encoded);
        }
        DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
        out.writeBytes(payload);
        out.flush();
        out.close();
        int responseCode = httpConnection.getResponseCode();
        LOGGER.info("responseCode = " + responseCode);
        LOGGER.info("responseMessage = " + httpConnection.getResponseMessage());
        httpConnection.disconnect();
    }

    private void initCluster() {
        String memoryQuota = "400";
        String indexMemoryQuota = "400";
        Boolean keyValue = true;
        Boolean query = true;
        Boolean index = true;
        Boolean fts = true;

        try {
            String urlBase = String.format("http://%s:%s", couchbase.getContainerIpAddress(), couchbase.getMappedPort(8091));

            String poolURL = urlBase + "/pools/default";
            String poolPayload = "memoryQuota=" + URLEncoder.encode(memoryQuota, "UTF-8") + "&indexMemoryQuota=" + URLEncoder.encode(indexMemoryQuota, "UTF-8");

            String setupServicesURL = urlBase + "/node/controller/setupServices";
            StringBuilder servicePayloadBuilder = new StringBuilder();
            if (keyValue) {
                servicePayloadBuilder.append("kv,");
            }
            if (query) {
                servicePayloadBuilder.append("n1ql,");
            }
            if (index) {
                servicePayloadBuilder.append("index,");
            }
            if (fts) {
                servicePayloadBuilder.append("fts,");
            }
            String setupServiceContent = "services=" + URLEncoder.encode(servicePayloadBuilder.toString(), "UTF-8");

            String webSettingsURL = urlBase + "/settings/web";
            String webSettingsContent = "username=" + URLEncoder.encode(user, "UTF-8") + "&password=" +
                    URLEncoder.encode(password, "UTF-8") + "&port=8091";

            String indexSettingsURL = urlBase + "/settings/indexes";
            String indexSettingsContent = "storageMode=plasma";

            callCouchbaseRestAPI(poolURL, poolPayload);
            callCouchbaseRestAPI(setupServicesURL, setupServiceContent);
            callCouchbaseRestAPI(webSettingsURL, webSettingsContent);
            callCouchbaseRestAPI(indexSettingsURL, indexSettingsContent, user, password, "POST");
            Thread.sleep(1000L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createFullTextIndex(GenericContainer couchbase) {
        String urlBaseIndex = String.format("http://%s:%s", couchbase.getContainerIpAddress(), couchbase.getMappedPort(8094));
        String fullTextIndexURL = urlBaseIndex + "/api/index/index-diana";
        StringBuilder fullTextIndexDiana = new StringBuilder();

        try {
            Path path = Paths.get(getClass().getClassLoader()
                    .getResource(INDEX_JSON).toURI());
            Stream<String> lines = Files.lines(path);
            lines.forEach(line -> fullTextIndexDiana.append(line));
            lines.close();

            callCouchbaseRestAPI(fullTextIndexURL, fullTextIndexDiana.toString(),
                    user, password, "PUT", "application/json");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createBucket() {
        CouchbaseCluster.create();
        CouchbaseCluster cluster = CouchbaseCluster.create(couchbase.getContainerIpAddress())
                .authenticate(user, password);
        ClusterManager clusterManager = cluster.clusterManager();
        BucketSettings bucketSettings = DefaultBucketSettings.builder()
                .enableFlush(true).name(BUCKET_NAME).quota(100).replicas(0)
                .type(BucketType.COUCHBASE).build();
        clusterManager.insertBucket(bucketSettings);
        Bucket bucket = cluster.openBucket(BUCKET_NAME);
        bucket.query(N1qlQuery.simple("CREATE PRIMARY INDEX index_" + BUCKET_NAME + " on " + BUCKET_NAME));
        bucket.close();
    }

    private WaitStrategy getCompositeWaitStrategy() {
        return new WaitAllStrategy()
                .withStrategy(new HttpWaitStrategy()
                        .forPort(8091)
                        .forPath("/ui/index.html")
                        .forStatusCode(200));
    }

    public GenericContainer getContainer() {
        return couchbase;
    }

}
