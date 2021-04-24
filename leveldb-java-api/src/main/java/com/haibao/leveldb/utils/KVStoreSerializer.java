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

    public final <T> T deserialize(byte[] data, Class<T> klass){
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
