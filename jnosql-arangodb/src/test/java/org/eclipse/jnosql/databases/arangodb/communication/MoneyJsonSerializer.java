package org.eclipse.jnosql.databases.arangodb.communication;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class MoneyJsonSerializer extends JsonSerializer<Money> {

    @Override
    public void serialize(Money value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

    }
}
