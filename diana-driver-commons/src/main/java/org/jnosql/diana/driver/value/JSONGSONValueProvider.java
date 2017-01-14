package org.jnosql.diana.driver.value;


import org.jnosql.diana.api.Value;

import java.util.Objects;

/**
 * The implementation that uses {@link JSONGSONValue}
 */
public class JSONGSONValueProvider implements JSONValueProvider {

    @Override
    public Value of(String json) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(json, "Json is required");
        return JSONGSONValue.of(json);
    }

    @Override
    public Value of(byte[] json) throws NullPointerException, UnsupportedOperationException {
        Objects.requireNonNull(json, "Json is required");
        return JSONGSONValue.of(String.valueOf(json));
    }
}
