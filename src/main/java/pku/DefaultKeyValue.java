package pku;

import java.util.HashMap;
import java.util.Set;

/**
 * 一个Key-Value的实现
 */
public class DefaultKeyValue implements KeyValue{
    private final HashMap<String, Object> kvs = new HashMap<>();

    public Object getObj(String key) {
        return kvs.get(key);
    }

    public HashMap<String, Object> getMap(){
        return kvs;
    }

    public DefaultKeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    public int getInt(String key) {
        return (Integer) kvs.getOrDefault(key, 0);
    }

    public long getLong(String key) {
        return (Long) kvs.getOrDefault(key, 0L);
    }

    public double getDouble(String key) {
        return (Double) kvs.getOrDefault(key, 0.0d);
    }

    public String getString(String key) {
        return (String) kvs.getOrDefault(key, null);
    }

    public Set<String> keySet() {
        return kvs.keySet();
    }

    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
}
