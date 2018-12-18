package pku;

import java.util.HashMap;
import java.util.Set;

/**
 * Key-Value接口
 * 代表key-value的映射, key为String类型, value的类型只有四种:int long double String
 */
public interface KeyValue {

    public Object getObj(String key);

    public HashMap<String, Object> getMap();

    public KeyValue put(String key, int value) ;

    public KeyValue put(String key, long value);

    public KeyValue put(String key, double value);

    public KeyValue put(String key, String value);

    public int getInt(String key);

    public long getLong(String key);

    public double getDouble(String key);

    public String getString(String key);

    public Set<String> keySet();

    public boolean containsKey(String key);
}
