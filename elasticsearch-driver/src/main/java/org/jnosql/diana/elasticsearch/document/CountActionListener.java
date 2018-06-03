package org.jnosql.diana.elasticsearch.document;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.jnosql.diana.api.JNoSQLException;

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
        throw new ElasticsearchException("An error when do search from QueryBuilder on elasticsearch", e);
    }
}
