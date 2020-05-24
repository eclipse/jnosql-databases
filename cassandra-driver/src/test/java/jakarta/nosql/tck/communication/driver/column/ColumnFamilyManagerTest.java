package jakarta.nosql.tck.communication.driver.column;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnFamilyManagerTest {

    @ParameterizedTest
    @ColumnSource("column_insert.properties")
    public void parameterizedTest(ColumnArgument argument) {
        Assumptions.assumeFalse(argument.isEmpty());
        assertNotNull(argument);
    }

    @ParameterizedTest
    @ColumnSource("empty")
    public void parameterizedTest3(ColumnArgument argument) {
        Assumptions.assumeFalse(argument.isEmpty(), "The you put the  file in the resources to activate this test");
        assertNotNull(argument);
    }
}
