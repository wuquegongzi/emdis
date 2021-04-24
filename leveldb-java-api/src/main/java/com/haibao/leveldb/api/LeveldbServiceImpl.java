package com.haibao.leveldb.api;

import cn.hutool.core.lang.Console;
import cn.hutool.core.util.ObjectUtil;
import com.haibao.leveldb.api.builder.LocalOptions;
import com.haibao.leveldb.utils.GenericsUtils;
import com.haibao.leveldb.utils.KVStoreSerializer;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;

/**
 * Leveldb java 客户端
 *
 * @author ml.c
 * @date 5:09 PM 4/23/21
 **/
public class LeveldbServiceImpl<K, V> implements LeveldbService<K, V> {

    KVStoreSerializer kvStoreSerializer = new KVStoreSerializer();

    private static final String PATH = "cache/data/emdisdb";
    private static final File FILE = new File(PATH);

    private static DBFactory factory;
    private static Options options;

    static {
        factory = new Iq80DBFactory();

        Logger logger = new Logger() {
            @Override
            public void log(String message) {
                Console.log(message);
            }
        };
        options = new LocalOptions.Builder()
                .createIfMissing(true)
                .blockSize(8092)
                .logger(logger)
                // 100MB cache
                .cacheSize(100 * 1048576).build();
    }

    /**
     * 获取DB
     *
     * @return
     * @throws IOException
     */
    private DB getDB() {
        DB db = null;
        try {
            db = factory.open(FILE, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return db;
    }

    /**
     * 关闭 DB
     */
    private void close(DB db) {
        if (null != db) {
            try {
                db.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 校验key
     *
     * @param k
     */
    private void checkNull(Object k) {
        if (ObjectUtil.isEmpty(k)) {
            throw new IllegalArgumentException("key is null,please check it!");
        }
    }

    @Override
    public void set(K k, V v) {
        DB db = null;
        try {
            db = getDB();
            checkNull(k);
            db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v));
        } finally {
            close(db);
        }
    }

    @Override
    public void setSync(K k, V v) {
        DB db = null;
        try {
            checkNull(k);
            db = getDB();
            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v), writeOptions);
        } finally {
            close(db);
        }
    }

    @Override
    public void setBatch(Map<K, V> map) {
        DB db = null;
        WriteBatch writeBatch = null;
        try {
            db = getDB();
            writeBatch = db.createWriteBatch();

            for (K key : map.keySet()) {
                writeBatch.put(kvStoreSerializer.serialize(key), kvStoreSerializer.serialize(map.get(key)));
            }
            db.write(writeBatch);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != writeBatch) {
                    writeBatch.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            close(db);
        }
    }

    @Override
    public Object get(Object o) {
        DB db = null;
        Object obj = null;
        try {
            checkNull(o);
            db = getDB();
            byte[] data = db.get(kvStoreSerializer.serialize(o));
            obj = kvStoreSerializer.deserialize(data, Object.class);
        } finally {
            close(db);
        }
        return obj;
    }

    @Override
    public V get(K k, Class<V> vClass) {
        DB db = null;
        V obj = null;
        try {
            checkNull(k);
            db = getDB();
            byte[] data = db.get(kvStoreSerializer.serialize(k));
            obj = kvStoreSerializer.deserialize(data, vClass);
        } finally {
            close(db);
        }

        return obj;
    }

    @Override
    public boolean remove(K k) {
        DB db = null;
        try {
            checkNull(k);
            db = getDB();
            db.delete(kvStoreSerializer.serialize(k));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            close(db);
        }
        return true;
    }

    @Override
    public boolean removeSync(K k) {

        DB db = null;
        try {
            checkNull(k);
            db = getDB();
            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.delete(kvStoreSerializer.serialize(k), writeOptions);
        } finally {
            close(db);
        }

        return false;
    }

    @Override
    public boolean removeBatch(Set<K> set) {

        DB db = null;
        WriteBatch writeBatch = null;
        try {
            db = getDB();
            writeBatch = db.createWriteBatch();
            for (K k : set) {
                writeBatch.delete(kvStoreSerializer.serialize(k));
            }
            db.write(writeBatch);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (null != writeBatch) {
                    writeBatch.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            close(db);
        }
        return true;
    }


    @Override
    public LinkedHashMap scan(K startKey, K endKey, int limit) {
        //todo
        return null;
    }

    @Override
    public Snapshot getSnapshot() {
        //todo
        return null;
    }

    @Override
    public boolean repairDB(String dbDir, Options options) {
        //todo
        return false;
    }

}
