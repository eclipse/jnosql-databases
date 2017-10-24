package org.jnosql.diana.cassandra.column;

public interface UDTFinisherBuilder {

    /**
     * Creates a udt instance
     *
     * @return a udt instance
     * @throws IllegalStateException when there is a null element
     */
    public UDT build();
}
