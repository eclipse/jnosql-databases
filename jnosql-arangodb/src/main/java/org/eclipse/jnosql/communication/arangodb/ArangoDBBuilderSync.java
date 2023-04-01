/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.arangodb;

import com.arangodb.ArangoDB;
import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;

public class ArangoDBBuilderSync implements ArangoDBBuilder {

    private final ArangoDB.Builder arangoDB;

    ArangoDBBuilderSync(ArangoDB.Builder arangoDB) {
        this.arangoDB = arangoDB;
    }

    @Override
    public void host(String host, int port) {
        arangoDB.host(host, port);
    }

    @Override
    public void timeout(int timeout) {
        arangoDB.timeout(timeout);
    }

    @Override
    public void user(String user) {
        arangoDB.user(user);
    }

    @Override
    public void password(String password) {
        arangoDB.password(password);
    }

    @Override
    public void useSsl(boolean useSsl) {
        arangoDB.useSsl(useSsl);
    }


    @Override
    public void chunksize(int chunksize) {
        arangoDB.chunksize(chunksize);
    }

    @Override
    public void maxConnections(int maxConnections) {
        arangoDB.maxConnections(maxConnections);
    }

    @Override
    public void useProtocol(Protocol protocol) {
        arangoDB.useProtocol(protocol);
    }

    @Override
    public void acquireHostList(boolean acquireHostList) {
        arangoDB.acquireHostList(acquireHostList);
    }

    @Override
    public void loadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy) {
        arangoDB.loadBalancingStrategy(loadBalancingStrategy);
    }

    public ArangoDB build() {
        return arangoDB.build();
    }
}
