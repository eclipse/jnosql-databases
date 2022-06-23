/*
 *  Copyright (c) 2017-2019 Otávio Santana and others
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
 *   Jesse Gallagher
 */
package org.eclipse.jnosql.communication.driver.attachment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Default representation of {@link EntityAttachment} for in-memory data.
 * 
 * @since 0.0.9
 */
public class ByteArrayEntityAttachment implements EntityAttachment {
    private final String name;
    private final String contentType;
    private final long lastModified;
    private final byte[] data;
    
    public ByteArrayEntityAttachment(String name, String contentType, long lastModified, byte[] data) {
        this.name = name;
        this.contentType = contentType;
        this.lastModified = lastModified;
        this.data = data;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public InputStream getData() throws IOException {
        return new ByteArrayInputStream(data);
    }
    
    @Override
    public long getLength() {
        return data.length;
    }

}
