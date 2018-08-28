/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document.configuration;

import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.SettingsBuilder;
import org.jnosql.diana.couchdb.document.CouchDBDocumentCollectionManagerFactory;
import org.jnosql.diana.couchdb.document.CouchDBDocumentConfiguration;

public class CouchDBDocumentTcConfiguration extends CouchDBDocumentConfiguration {

	private static CouchDBDocumentTcConfiguration tcConfiguration;
	private CouchDBContainer couchDB;

	private CouchDBDocumentTcConfiguration() {
		couchDB = new CouchDBContainer();
		couchDB.start();
	}

	public static CouchDBDocumentTcConfiguration getTcConfiguration() {
		if (tcConfiguration == null) {
			tcConfiguration = new CouchDBDocumentTcConfiguration();
		}
		return tcConfiguration;
	}

	@Override
	public CouchDBDocumentCollectionManagerFactory get() {
		SettingsBuilder builder = Settings.builder();
		builder.put(CouchDBDocumentConfiguration.PORT, couchDB.getFirstMappedPort());
		return super.get(builder.build());
	}

}
