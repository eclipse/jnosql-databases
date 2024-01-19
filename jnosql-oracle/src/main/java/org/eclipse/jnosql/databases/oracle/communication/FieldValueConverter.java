package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;

import java.lang.reflect.Array;
import java.util.Map;

enum FieldValueConverter {

    INSTANCE;

    FieldValue of(Object value){
        if(value == null){
            return NullValue.getInstance();
        }
        if (value instanceof String string) {
            return new StringValue(string);
        } else if (value instanceof Integer integer) {
            return new IntegerValue(integer);
        } else if (value instanceof Long longValue) {
            return new LongValue(longValue);
        } else if (value instanceof Double doubleValue) {
            return new DoubleValue(doubleValue);
        } else if (value instanceof Boolean booleanValue) {
            return Boolean.TRUE.equals(booleanValue) ? BooleanValue.trueInstance() : BooleanValue.falseInstance();
        } else if (value instanceof Number) {
            return new NumberValue(value.toString());
        }  else if (value instanceof byte[]) {
            return new BinaryValue((byte[]) value);
        } else if (value instanceof Iterable<?> values) {
            return createList(values);
        } else if (value.getClass().isArray()) {
            return createArray(value);
        } else if (value instanceof Map<?,?>) {
            return entries((Map<String, ?>) value);
        }else if (value instanceof FieldValue) {
            return (FieldValue) value;
        } else {
            throw new UnsupportedOperationException("There is not support to: " + value.getClass());
        }
    }

    private MapValue entries(Map<String, ?> value) {
        MapValue mapValue = new MapValue();
        for (Map.Entry<String, ?> entry : value.entrySet()) {
            mapValue.put(entry.getKey(), of(entry.getValue()));
        }
        return mapValue;
    }

    private ArrayValue createArray(Object value) {
        var arrayValue = new ArrayValue();
        int length = Array.getLength(value);
        for (int i = 0; i < length; i ++) {
            arrayValue.add(of(Array.get(value, i)));
        }
        return arrayValue;
    }

    private ArrayValue createList(Iterable<?> values) {
        var arrayValue = new ArrayValue();
        for (Object value : values) {
            arrayValue.add(of(value));
        }
        return arrayValue;
    }
}
