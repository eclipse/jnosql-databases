package org.eclipse.jnosql.communication.couchbase.document;

import jakarta.nosql.Sort;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentQuery;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

final class DeleteQueryWrapper implements DocumentQuery {

    private final DocumentDeleteQuery query;

    private DeleteQueryWrapper(DocumentDeleteQuery query) {
        this.query = query;
    }

    @Override
    public long getLimit() {
        return 0;
    }

    @Override
    public long getSkip() {
        return 0;
    }

    @Override
    public String getDocumentCollection() {
        return query.getDocumentCollection();
    }

    @Override
    public Optional<DocumentCondition> getCondition() {
        return this.query.getCondition();
    }

    @Override
    public List<Sort> getSorts() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getDocuments() {
        return Collections.emptyList();
    }

    static DocumentQuery of(DocumentDeleteQuery query) {
        return new DeleteQueryWrapper(query);
    }
}
