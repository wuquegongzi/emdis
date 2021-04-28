package com.haibao.leveldb.api;

import cn.hutool.core.util.ObjectUtil;
import com.haibao.leveldb.api.builder.LeveldbEnums;
import com.haibao.leveldb.api.builder.LocalLeveldbClient;
import com.haibao.leveldb.utils.KVStoreSerializer;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * Leveldb java 客户端
 *
 * @author ml.c
 * @date 5:09 PM 4/23/21
 **/
public class LeveldbServiceImpl<K, V> implements LeveldbService<K, V> {

    //init KVStoreSerializer
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

    private void checkMapNull(Map<K, V> map) {
        if (map.containsKey(null)) {
            throw new IllegalArgumentException("parms map contains null key,please check it!");
        }
    }

    @Override
    public void set(K k, V v) {
        try {
            checkNull(k);
            db.put(kvStoreSerializer.serialize(k), kvStoreSerializer.serialize(v));
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
        } catch (DBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setBatch(Map<K, V> map, boolean syn) {

        checkMapNull(map);
        WriteBatch writeBatch = null;
        try {
            writeBatch = db.createWriteBatch();

            for (K key : map.keySet()) {
                writeBatch.put(kvStoreSerializer.serialize(key), kvStoreSerializer.serialize(map.get(key)));
            }

            if (syn) {
                WriteOptions writeOptions = new WriteOptions().sync(true);
                db.write(writeBatch, writeOptions);
            } else {
                db.write(writeBatch);
            }
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
        return kvStoreSerializer.deserialize(data);
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean removeBatch(Set<K> set, boolean syn) {

        WriteBatch writeBatch = null;
        try {
            writeBatch = db.createWriteBatch();
            for (K k : set) {
                writeBatch.delete(kvStoreSerializer.serialize(k));
            }

            if (syn) {
                WriteOptions writeOptions = new WriteOptions().sync(true);
                db.write(writeBatch, writeOptions);
            } else {
                db.write(writeBatch);
            }
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
        return LocalLeveldbClient.repairDB(dbDir, options);
    }

    @Override
    public void removeAll() {

        DBIterator iterator = db.iterator();
        try {
            Set<String> set = new HashSet();
            int size = 0;
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                set.add(key);
                size++;
                if (size > 1000) {
                    removeBatchSyn(set);
                    set = new HashSet();
                    size = 0;
                }
            }

            if(size > 0 || set.size() > 0){
                removeBatchSyn(set);
            }
//            System.out.println(size);
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private boolean removeBatchSyn(Set<String> set) {

        WriteBatch writeBatch = null;
        try {
            writeBatch = db.createWriteBatch();
            for (String k : set) {
                writeBatch.delete(kvStoreSerializer.serialize(k));
            }

            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.write(writeBatch, writeOptions);

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
    public long size() {

        try {
            byte[] data = db.get(kvStoreSerializer.serialize(LocalLeveldbClient.EMDIS_CONFIG_KEY));
            if (null != data && data.length > 0) {
                Map<String, Object> configMap = kvStoreSerializer.deserialize(data, Map.class);
                if (null != configMap && configMap.containsKey(LeveldbEnums.SIZE.getProperty())) {
                    return (Integer) configMap.get(LeveldbEnums.SIZE.getProperty());
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
            Range range = new Range(bytes(key), bytes(keyMap.get(key)));
            rangeList.add(range);
        }
        Range ranges[] = (Range[]) rangeList.toArray();
        long[] sizes = db.getApproximateSizes(ranges);

        return sizes;
    }

    @Override
    public boolean status() {
        try {
            String stats = db.getProperty("leveldb.stats");
            if (null == stats) {
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
