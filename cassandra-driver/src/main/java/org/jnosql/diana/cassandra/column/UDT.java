package org.jnosql.diana.cassandra.column;


import org.jnosql.diana.api.TypeSupplier;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.column.Column;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UDT implements Column {

    private final String name;

    private final String userType;

    private final List<Column> columns;

    public UDT(String name, String userType, List<Column> columns) {
        this.name = name;
        this.userType = userType;
        this.columns = columns;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Value getValue() {
        return Value.of(columns);
    }

    @Override
    public <T> T get(Class<T> clazz) throws NullPointerException, UnsupportedOperationException {
        if (Iterable.class.isAssignableFrom(clazz)) {
            return (T) columns;
        }
        throw new IllegalArgumentException("This method just returns a list of document");
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public Object get() {
        return columns;
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public String getUserType() {
        return userType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UDT)) {
            return false;
        }
        UDT udt = (UDT) o;
        return Objects.equals(name, udt.name) &&
                Objects.equals(userType, udt.userType) &&
                Objects.equals(columns, udt.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, userType, columns);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UDT{");
        sb.append("name='").append(name).append('\'');
        sb.append(", userType='").append(userType).append('\'');
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
