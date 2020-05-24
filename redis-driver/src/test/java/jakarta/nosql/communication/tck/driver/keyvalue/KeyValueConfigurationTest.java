package jakarta.nosql.communication.tck.driver.keyvalue;

import jakarta.nosql.keyvalue.KeyValueConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyValueConfigurationTest {


    @Test
    public void shouldReturnFromConfiguration() {
        KeyValueConfiguration configuration = KeyValueConfiguration.getConfiguration();
        assertNotNull(configuration);
        Assertions.assertTrue(configuration instanceof KeyValueConfiguration);
    }

    @Test
    public void shouldReturnErrorWhenParameterIsNull() {
        KeyValueConfiguration configuration = KeyValueConfiguration.getConfiguration();
        assertThrows(NullPointerException.class, ()-> configuration.get(null));
    }
}
