package com.haibao.leveldb.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;

/**
 * Leveldb 客户端接口
 * @param <T>
 */
public interface LeveldbService<T> {

    /**
     *
     * @param k
     * @param v
     */
    void set(String k,T v);

    /**
     *
     * @param k
     * @param v
     */
    void setSync(String k,T v);

    /**
     *
     * @param map
     */
    void setBatch(Map<String,T> map);

    /**
     *
     * @param k
     * @return
     */
    T get(String k);

    /**
     *
     * @param k
     * @return
     */
    boolean remove(String k);

    /**
     *
     * @param k
     * @return
     */
    boolean removeSync(String k);

    /**
     *
     * @param set
     * @return
     */
    boolean removeBatch(Set<String> set);

    /**
     *
     * @param startKey
     * @param endKey
     * @param limit
     * @return
     */
    LinkedHashMap scan(String startKey, String endKey, int limit);

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


}
