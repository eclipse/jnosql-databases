package org.jnosql.diana.riak.key;

import java.lang.reflect.Type;
import java.util.Objects;

import org.jnosql.diana.api.TypeSupplier;
import org.jnosql.diana.api.Value;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

final public class RiakValue implements Value {

	private final Gson gson;
	
	private final String json;
	
	public RiakValue(Gson gson, String json) {
		this.gson = gson;
		this.json = json;
	}

    public static Value of(Gson gson, String json) {
        return new RiakValue(gson, json);
    }

    @Override
    public Object get() {
        return json;
    }


    @Override
    public <T> T get(Class<T> clazz) throws NullPointerException, UnsupportedOperationException {
        return gson.fromJson(json, clazz);
    }

    @Override
    public <T> T get(TypeSupplier<T> typeSupplier) throws NullPointerException, UnsupportedOperationException {
        Type type = new TypeToken<T>() { }.getType();
        return gson.fromJson(json, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RiakValue that = (RiakValue) o;
        return Objects.equals(json, that.json);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(json);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RedisValue{");
        sb.append("gson=").append(gson);
        sb.append(", json='").append(json).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
