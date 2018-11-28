package pku;

/**
 * 字节消息接口
 *
 */
public interface ByteMessage {

    //设置消息头
    void setHeaders(KeyValue headers);
    //获取字节数据
    byte[] getBody();
    //设置字节数据
    void setBody(byte[] body);

    public KeyValue headers();

    //设置header
    public ByteMessage putHeaders(String key, int value);

    public ByteMessage putHeaders(String key, long value);

    public ByteMessage putHeaders(String key, double value);

    public ByteMessage putHeaders(String key, String value) ;

}
