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
package org.eclipse.jnosql.mapping.mongodb;


import jakarta.inject.Inject;
import org.bson.types.ObjectId;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Music.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
public class DocumentEntityConverterTest {

    @Inject
    private DocumentEntityConverter converter;

    @Test
    public void shouldConverterToDocument() {
        ObjectId id = new ObjectId();
        Music music = new Music(id.toString(), "Music", 2021);
        DocumentEntity entity = converter.toDocument(music);
        Assertions.assertNotNull(entity);
        Assertions.assertEquals(Music.class.getSimpleName(), entity.name());
        Assertions.assertEquals(id, entity.find("_id", ObjectId.class).get());
        Assertions.assertEquals("Music", entity.find("name", String.class).get());
        Assertions.assertEquals(2021, entity.find("year", int.class).get());
    }

    @Test
    public void shouldConvertToEntity() {
        ObjectId id = new ObjectId();
        DocumentEntity entity = DocumentEntity.of("Music");
        entity.add("name", "Music");
        entity.add("year", 2022);
        entity.add("_id", id);

        Music music = converter.toEntity(entity);
        Assertions.assertNotNull(music);
        Assertions.assertEquals("Music", music.getName());
        Assertions.assertEquals(2022, music.getYear());
        Assertions.assertEquals(id.toString(), music.getId());
    }
}
