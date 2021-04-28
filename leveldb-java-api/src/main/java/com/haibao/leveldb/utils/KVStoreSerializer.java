package com.haibao.leveldb.utils;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * KV 存储 序列化
 *
 * @author ml.c
 * @date 9:35 PM 4/24/21
 **/
public class KVStoreSerializer {

    public final byte[] serialize(Object o){
        if (o instanceof String) {
            return bytes((String) o);
        } else {
            return ObjectAndByte.toByteArray(o);
        }
    }

    public final <T> T deserialize(byte[] data){

        if(null == data || data.length <= 0){
            return null;
        }

        // 不足6位，直接认为是字符串，,经测试单个字符序列化后的byte[]也有8位
        if (data.length < 6)
        {
            return (T) asString(data);
        }

        String protocol = Integer.toHexString(data[0] & 0x000000ff) + Integer.toHexString(data[1] & 0x000000ff);

        T obj;
        // 如果是jdk序列化后的
        if ("ACED".equals(protocol.toUpperCase())){
            obj = (T) ObjectAndByte.toObject(data);
            if (obj != null){
                return obj;
            }
        }

        return (T) asString(data);
    }

    public final <T> T deserialize(byte[] data, Class<T> klass){
        if(null == data || data.length <= 0){
            return null;
        }
        if (String.class.equals(klass)) {
            return (T) asString(data);
        } else {
            return (T) ObjectAndByte.toObject(data);
        }
    }

    final byte[] serialize(long value) {
        return bytes(String.valueOf(value));
    }

    final long deserializeLong(byte[] data) {
        return Long.parseLong(asString(data));
    }

}
