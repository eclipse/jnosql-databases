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
package org.jnosql.diana.arangodb;

import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;

interface ArangoDBBuilder {


    /**
     * @param host the host
     * @deprecated use {@link ArangoDBBuilder#host(String, int)} instead
     */
    void host(String host);

    /**
     * * @deprecated use {@link ArangoDBBuilder#host(String, int)} instead
     *
     * @param port the port
     */
    void port(int port);

    void host(String host, int port);

    void timeout(int timeout);

    void user(String user);

    void password(String password);

    void useSsl(boolean useSsl);

    void chunksize(int chunksize);

    void maxConnections(int maxConnections);

    void useProtocol(Protocol protocol);

    void acquireHostList(boolean acquireHostList);

    void loadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy);
}
