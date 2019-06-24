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

package org.jnosql.diana.elasticsearch.document;

import jakarta.nosql.CommunicationException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;

import java.util.function.Consumer;

final class CountActionListener implements ActionListener<SearchResponse> {

    private final Consumer<Long> callback;
    private final String documentCollection;

    CountActionListener(Consumer<Long> callback, String documentCollection) {
        this.callback = callback;
        this.documentCollection = documentCollection;
    }

    @Override
    public void onResponse(SearchResponse response) {
        callback.accept(response.getHits().getTotalHits());
    }

    @Override
    public void onFailure(Exception e) {
        throw new CommunicationException("An error when do count on document collection: " + documentCollection, e);
    }
}
