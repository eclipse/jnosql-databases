package org.eclipse.jnosql.databases.arangodb.communication;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;

public class MoneyJsonDeserializer extends JsonDeserializer<Money> {

    @Override
    public Money deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JacksonException {

        JsonNode rootNode = jsonParser.getCodec().readTree(jsonParser);
        JsonNode currencyNode = rootNode.get("currency");
        JsonNode valueNode = rootNode.get("value");
        String currency = "USD";
        BigDecimal amount = BigDecimal.ZERO;
        if (currencyNode != null && currencyNode.isTextual()) {
            currency = currencyNode.asText();
        }

        if (valueNode != null && valueNode.isNumber()) {
            amount = valueNode.decimalValue();
        }

        return new Money(currency, amount);
    }
}
