package com.haibao.leveldb.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.haibao.leveldb.api.LeveldbService;
import com.haibao.leveldb.api.LeveldbServiceImpl;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * 自定义LeveldbCache，实现guava接口
 *
 * @author ml.c
 * @date 8:45 PM 4/24/21
 **/
public class LeveldbCache<K,V> implements Cache<K,V> {

    LeveldbService<K,V> leveldbService = new LeveldbServiceImpl<K,V>();

    @Override
    public V getIfPresent(Object o) {
        if(String.class.equals(o.getClass())){
            leveldbService.get(o);
        }else{

        }

        return null;
    }

    @Override
    public V get(K k, Callable<? extends V> callable) throws ExecutionException {

        return null;
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> iterable) {
        return null;
    }

    @Override
    public void put(K k, V v) {

    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

    }

    @Override
    public void invalidate(Object o) {

    }

    @Override
    public void invalidateAll(Iterable<?> iterable) {

    }

    @Override
    public void invalidateAll() {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return null;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return null;
    }

    @Override
    public void cleanUp() {

    }
}
