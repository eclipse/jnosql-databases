package jakarta.nosql.tck.communication.driver.column;

import jakarta.nosql.CommunicationException;

public class ColumnDriverException extends CommunicationException {

    public ColumnDriverException(String message) {
        super(message);
    }
}
