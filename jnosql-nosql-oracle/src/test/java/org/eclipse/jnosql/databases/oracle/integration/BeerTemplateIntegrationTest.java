/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.integration;


import jakarta.inject.Inject;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.databases.oracle.communication.Database;
import org.eclipse.jnosql.databases.oracle.communication.OracleNoSQLConfigurations;
import org.eclipse.jnosql.databases.oracle.mapping.OracleNoSQLTemplate;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Beer.class)
@AddPackages(OracleNoSQLTemplate.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class BeerTemplateIntegrationTest {

    @Inject
    private OracleNoSQLTemplate template;

    static {
        System.setProperty(OracleNoSQLConfigurations.HOST.get(), Database.INSTANCE.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), "library");
    }

    @Test
    void shouldInsert() {

        Beer beer = Beer.builder()
                .id(UUID.randomUUID().toString())
                .data(Map.of("name", "beer"))
                .comments(List.of("comment1", "comment2"))
                .crew(List.of(new Crew("Otavio")))
                .build();

        this.template.insert(beer);

        Optional<Beer> result = this.template.select(Beer.class).where("id").eq(beer.id()).singleResult();

        SoftAssertions.assertSoftly(soft ->{
            soft.assertThat(result).isPresent();
            Beer updateBeer = result.orElseThrow();
            soft.assertThat(updateBeer.id()).isEqualTo(beer.id());
            soft.assertThat(updateBeer.data()).isEqualTo(beer.data());
            soft.assertThat(updateBeer.comments()).isEqualTo(beer.comments());
            soft.assertThat(updateBeer.crew()).isEqualTo(beer.crew());
        });

    }

    


}
