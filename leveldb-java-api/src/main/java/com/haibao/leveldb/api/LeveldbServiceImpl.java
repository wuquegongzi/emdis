package com.haibao.leveldb.api;

import cn.hutool.core.util.ObjectUtil;
import com.haibao.leveldb.api.builder.LocalLeveldbClient;
import com.haibao.leveldb.utils.KVStoreSerializer;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

/**
 * Leveldb java 客户端
 *
 * @author ml.c
 * @date 5:09 PM 4/23/21
 **/
public class LeveldbServiceImpl<K, V> implements LeveldbService<K, V> {

    KVStoreSerializer kvStoreSerializer = new KVStoreSerializer();

    DB db = null;
    public LeveldbServiceImpl() {
        if( null == db ){
            synchronized (LeveldbServiceImpl.class){
                if(null == db ){
                    this.db = LocalLeveldbClient.getInstance();
                }
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
        checkNull(k);
        db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v));
    }

    @Override
    public void setSync(K k, V v) {
        checkNull(k);
        WriteOptions writeOptions = new WriteOptions().sync(true);
        db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v), writeOptions);
    }

    @Override
    public void setBatch(Map<K, V> map) {
        WriteBatch writeBatch = null;
        try {
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
        }
    }

    @Override
    public V get(Object o) {
        checkNull(o);
        byte[] data = db.get(kvStoreSerializer.serialize(o));
        return (V) kvStoreSerializer.deserialize(data,Object.class);
    }

    @Override
    public V get(K k, Class<V> vClass) {

        checkNull(k);
        byte[] data = db.get(kvStoreSerializer.serialize(k));
        return kvStoreSerializer.deserialize(data, vClass);
    }

    @Override
    public boolean remove(K k) {
        try {
            checkNull(k);
            db.delete(kvStoreSerializer.serialize(k));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean removeSync(K k) {
        try {
            checkNull(k);
            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.delete(kvStoreSerializer.serialize(k), writeOptions);
            return true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean removeBatch(Set<K> set) {

        WriteBatch writeBatch = null;
        try {
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

    @Override
    public void removeAll() {

    }

    @Override
    public long size() {
        return 0;
    }

}
