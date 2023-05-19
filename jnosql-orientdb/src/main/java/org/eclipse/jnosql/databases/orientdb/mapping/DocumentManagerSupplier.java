/*
 *  Copyright (c) 2022 Eclipse Contribuitor
 * All rights reserved. This program and the accompanying materials
 *  and Apache License v2.0 which accompanies this distribution.
 *  The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *  and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *    You may elect to redistribute this code under either of these licenses.
 */

package org.eclipse.jnosql.databases.orientdb.mapping;

import jakarta.data.exceptions.MappingException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBDocumentConfiguration;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBDocumentManager;
import org.eclipse.jnosql.databases.orientdb.communication.OrientDBDocumentManagerFactory;
import org.eclipse.jnosql.mapping.config.MicroProfileSettings;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.eclipse.jnosql.mapping.config.MappingConfigurations.DOCUMENT_DATABASE;

@ApplicationScoped
class DocumentManagerSupplier implements Supplier<OrientDBDocumentManager> {

    private static final Logger LOGGER = Logger.getLogger(DocumentManagerSupplier.class.getName());


    @Override
    @Produces
    @Typed(OrientDBDocumentManager.class)
    public OrientDBDocumentManager get() {
        Settings settings = MicroProfileSettings.INSTANCE;
        OrientDBDocumentConfiguration configuration = new OrientDBDocumentConfiguration();
        OrientDBDocumentManagerFactory factory = configuration.apply(settings);
        Optional<String> database = settings.get(DOCUMENT_DATABASE, String.class);
        String db = database.orElseThrow(() -> new MappingException("Please, inform the database filling up the property "
                + DOCUMENT_DATABASE.get()));
        OrientDBDocumentManager manager = factory.apply(db);
        LOGGER.log(Level.FINEST, "Starting  a OrientDBDocumentManager instance using Eclipse MicroProfile Config," +
                " database name: " + db);
        return manager;
    }

    public void close(@Disposes OrientDBDocumentManager manager) {
        LOGGER.log(Level.FINEST, "Closing OrientDBDocumentManager resource, database name: " + manager.name());
        manager.close();
    }

}
