package com.haibao.leveldb.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;

/**
 * Leveldb 客户端接口
 * @param <K,V>
 */
public interface LeveldbService<K,V> {

    /**
     *
     * @param k
     * @param v
     */
    void set(K k,V v);

    /**
     *
     * @param k
     * @param v
     */
    void setSync(K k,V v);

    /**
     *
     * @param map
     */
    void setBatch(Map<K,V> map,boolean syn);

    V get(Object o);

    V get(K k,Class<V> vClass);

    /**
     *
     * @param k
     * @return
     */
    boolean remove(K k);

    /**
     *
     * @param k
     * @return
     */
    boolean removeSync(K k);

    /**
     *
     * @param set  key的集合
     * @param syn  是否同时删除持久化
     * @return
     */
    boolean removeBatch(Set<K> set,boolean syn);

    /**
     *
     * @param startKey
     * @param endKey
     * @param limit
     * @return
     */
    LinkedHashMap scan(K startKey, K endKey, int limit);

    /**
     * 得到快照
     * @return
     */
    Snapshot getSnapshot();

    /**
     * 修复DB
     * @param dbDir
     * @param options
     * @return
     */
    boolean repairDB(String dbDir, Options options);

    void removeAll();

    long size();

    /**
     * GetApproximateSizes方法可以被用来得到一个或多个key range所占用的文件系统空间的近似大小
     * @param keyMap
     * @return
     */
    long[] getApproximateSizes(Map<String,String> keyMap);

    boolean status();

    /**
     * 销毁
     */
    void destroy();
}
