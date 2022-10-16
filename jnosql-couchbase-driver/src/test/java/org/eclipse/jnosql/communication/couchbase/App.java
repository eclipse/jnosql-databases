package org.eclipse.jnosql.communication.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;

import java.time.Duration;

public class App {

    public static void main(String[] args) {
        Cluster cluster = Cluster.connect("couchbase://localhost", "root", "123456");
        Bucket bucket = cluster.bucket("jnosql");
        Collection collection = bucket.collection("users");

        JsonObject jsonObject = JsonObject.create().put("name", "mike");
        collection.insert("my-document", jsonObject);
    }
}
