package jakarta.nosql.tck.communication.driver.column;

import java.util.Collections;
import java.util.List;

public final class ColumnArgument {

    static final ColumnArgument EMPTY = new ColumnArgument();

    private final String entity;

    private final List<String> query;

    private final String idName;

    private final boolean empty;

    ColumnArgument(String entity, List<String> query, String idName) {
        this.entity = entity;
        this.query = query;
        this.idName = idName;
        this.empty = true;
    }
    ColumnArgument() {
        this.entity = null;
        this.query = null;
        this.idName = null;
        this.empty = false;
    }


    public String getEntity() {
        return entity;
    }

    public List<String> getQuery() {
        if (query == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(query);
    }

    public String getIdName() {
        return idName;
    }

    public boolean isEmpty() {
        return empty;
    }

    @Override
    public String toString() {
        return "ColumnArgument{" +
                "entity='" + entity + '\'' +
                ", query=" + query +
                ", idName='" + idName + '\'' +
                '}';
    }
}
