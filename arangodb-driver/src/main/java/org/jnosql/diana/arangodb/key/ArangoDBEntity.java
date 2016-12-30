package org.jnosql.diana.arangodb.key;


import com.arangodb.entity.DocumentField;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.KeyValueEntity;

import java.io.Serializable;

class ArangoDBEntity implements Serializable {

    @DocumentField(DocumentField.Type.KEY)
    protected String key;

    protected Object value;

    public ArangoDBEntity(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Value toValue() {
        return Value.of(value);
    }
}
