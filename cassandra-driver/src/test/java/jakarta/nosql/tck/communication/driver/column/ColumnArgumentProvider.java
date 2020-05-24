package jakarta.nosql.tck.communication.driver.column;

import org.eclipse.jnosql.diana.driver.ConfigurationReader;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ColumnArgumentProvider implements ArgumentsProvider, AnnotationConsumer<ColumnSource> {

    private static final Logger LOGGER = Logger.getLogger(ColumnArgumentProvider.class.getName());

    private String value;

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
        LOGGER.info("Reading in the resource the file: " + value);

        final Map<String, String> map = get();
        if (map.isEmpty()) {
            return Stream.of(Arguments.of(ColumnArgument.EMPTY));
        }
        final String entity = map.get("entity");
        final String idName = map.get("id.name");
        final List<String> query = map.entrySet().stream()
                .filter(s -> s.getKey().startsWith("query"))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        return Stream.of(Arguments.of(new ColumnArgument(entity, query, idName)));
    }


    public Map<String, String> get() {
        try {
            Properties properties = new Properties();
            InputStream stream = ConfigurationReader.class.getClassLoader()
                    .getResourceAsStream(value);
            if (Objects.nonNull(stream)) {
                properties.load(stream);
                return properties.keySet().stream().collect(Collectors
                        .toMap(Object::toString, s -> properties.get(s).toString()));
            } else {
                LOGGER.info("Does not find " + value + " as resource, returning an empty configuration");
                return Collections.emptyMap();
            }

        } catch (IOException e) {
            LOGGER.fine("The file was not found: " + value);
            return Collections.emptyMap();
        }
    }

    @Override
    public void accept(ColumnSource columnSource) {
        this.value = columnSource.value();
    }
}
