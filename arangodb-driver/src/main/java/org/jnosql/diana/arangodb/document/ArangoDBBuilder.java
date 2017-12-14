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
package org.jnosql.diana.arangodb.document;

import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;

import javax.net.ssl.SSLContext;

interface ArangoDBBuilder {

    void host(String host);

    void port(int port);

    void host(final String host, final int port);

    void timeout(final Integer timeout);

    void user(final String user);

    void password(final String password);

    void useSsl(boolean useSsl);

    void sslContext(final SSLContext sslContext);

   void chunksize(final Integer chunksize);

    void maxConnections(int maxConnections);

   void useProtocol(final Protocol protocol);

   void acquireHostList(final Boolean acquireHostList);

    void loadBalancingStrategy(final LoadBalancingStrategy loadBalancingStrategy);
}
