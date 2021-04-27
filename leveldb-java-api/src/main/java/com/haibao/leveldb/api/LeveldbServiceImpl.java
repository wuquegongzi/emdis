package com.haibao.leveldb.api;

import cn.hutool.core.util.ObjectUtil;
import com.google.common.collect.Maps;
import com.haibao.leveldb.api.builder.LeveldbEnums;
import com.haibao.leveldb.api.builder.LocalLeveldbClient;
import com.haibao.leveldb.utils.KVStoreSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

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
        if (null == db) {
            synchronized (LeveldbServiceImpl.class) {
                if (null == db) {
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

    private void checkMapNull(Map<K,V> map) {
        if(map.containsKey(null)){
            throw new IllegalArgumentException("parms map contains null key,please check it!");
        }
    }

    /**
     * 更新emdis自定义配置项
     * @param leveldbEnums
     * @param v
     */
    private void updateEmdisConfiguration(LeveldbEnums leveldbEnums, Object v) {
        Map<String,Object> configMap;

        byte[] data = db.get(kvStoreSerializer.serialize(LocalLeveldbClient.EMDIS_CONFIG_KEY));
        if(null == data || data.length < 1){
            configMap = Maps.newHashMap();
            configMap.put(leveldbEnums.getProperty(),v);
        }else{
            configMap = kvStoreSerializer.deserialize(data, Map.class);
            if(configMap.containsKey(leveldbEnums.getProperty())){
                Object objVal = configMap.get(leveldbEnums.getProperty());
                if(LeveldbEnums.SIZE.equals(leveldbEnums)){
                    int intVal = (Integer) objVal;
                    configMap.put(leveldbEnums.getProperty(),intVal+(Integer) v);
                }else{
                    //扩展 todo
                }
            } else {
                configMap.put(leveldbEnums.getProperty(),v);
            }
        }

        db.put(kvStoreSerializer.serialize(LocalLeveldbClient.EMDIS_CONFIG_KEY), kvStoreSerializer.serialize(configMap));
    }

    @Override
    public void set(K k, V v) {
        try {
            checkNull(k);
            db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v));
            updateEmdisConfiguration(LeveldbEnums.SIZE,1);
        } catch (DBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setSync(K k, V v) {
        try {
            checkNull(k);
            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v), writeOptions);
            updateEmdisConfiguration(LeveldbEnums.SIZE,1);
        } catch (DBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setBatch(Map<K, V> map,boolean syn) {

        checkMapNull(map);
        WriteBatch writeBatch = null;
        try {
            writeBatch = db.createWriteBatch();

            for (K key : map.keySet()) {
                writeBatch.put(kvStoreSerializer.serialize(key), kvStoreSerializer.serialize(map.get(key)));
            }

            if(syn){
                WriteOptions writeOptions = new WriteOptions().sync(true);
                db.write(writeBatch,writeOptions);
            }else{
                db.write(writeBatch);
            }

            updateEmdisConfiguration(LeveldbEnums.SIZE,map.size());
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
        V v = null;
        checkNull(o);
        byte[] data = db.get(kvStoreSerializer.serialize(o));
        try {
            v = (V) kvStoreSerializer.deserialize(data, Object.class);
        } catch (Exception e) {
//            e.printStackTrace();
        }
        //todo 判断 byte[] 类型
        if(null == v){
            v = (V) kvStoreSerializer.deserialize(data, String.class);
        }
        return v;
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
            updateEmdisConfiguration(LeveldbEnums.SIZE,-1);
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
            updateEmdisConfiguration(LeveldbEnums.SIZE,-1);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean removeBatch(Set<K> set,boolean syn) {

        WriteBatch writeBatch = null;
        try {
            writeBatch = db.createWriteBatch();
            for (K k : set) {
                writeBatch.delete(kvStoreSerializer.serialize(k));
            }

            if(syn){
                WriteOptions writeOptions = new WriteOptions().sync(true);
                db.write(writeBatch,writeOptions);
            }else{
                db.write(writeBatch);
            }
            updateEmdisConfiguration(LeveldbEnums.SIZE,set.size());
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
        return db.getSnapshot();
    }

    @Override
    public boolean repairDB(String dbDir, Options options) {
        return LocalLeveldbClient.repairDB(dbDir,options);
    }

    @Override
    public void removeAll() {
        // todo
    }

    @Override
    public long size() {

        try {
            byte[] data = db.get(kvStoreSerializer.serialize(LocalLeveldbClient.EMDIS_CONFIG_KEY));
            if(null != data && data.length > 0){
                Map<String, Object> configMap = kvStoreSerializer.deserialize(data, Map.class);
                if (null != configMap && configMap.containsKey(LeveldbEnums.SIZE.getProperty())) {
                    return ((Integer) configMap.get(LeveldbEnums.SIZE.getProperty()) - 1);
                }
            }
        } catch (DBException e) {
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public long[] getApproximateSizes(Map<String, String> keyMap) {

        List<Range> rangeList = new LinkedList();
        for (String key : keyMap.keySet()) {
            Range range = new Range(bytes(key),bytes(keyMap.get(key)));
            rangeList.add(range);
        }
        Range ranges[] = (Range[])rangeList.toArray();
        long[] sizes = db.getApproximateSizes(ranges);

        return sizes;
    }

    @Override
    public boolean status() {
        try {
            String stats = db.getProperty("leveldb.stats");
            if(null == stats){
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void destroy() {
        LocalLeveldbClient.destroy();
    }

}
